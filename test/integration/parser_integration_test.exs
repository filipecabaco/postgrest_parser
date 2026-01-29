defmodule PostgrestParser.Integration.ParserIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias PostgrestParser.SchemaCache
  alias PostgrestParser.TestConnection

  @tenant_id "integration_test"

  setup_all do
    {:ok, conn} = TestConnection.start_connection()

    :ok = SchemaCache.refresh(@tenant_id, conn)

    on_exit(fn ->
      SchemaCache.clear(@tenant_id)
    end)

    {:ok, conn: conn}
  end

  describe "schema introspection" do
    test "discovers tables from seeded schema" do
      assert {:ok, table} = SchemaCache.get_table(@tenant_id, "public", "customers")
      assert table.name == "customers"
      refute table.is_view
      assert Enum.any?(table.columns, &(&1.name == "name"))
      assert Enum.any?(table.columns, &(&1.name == "email"))
    end

    test "discovers primary keys" do
      assert {:ok, table} = SchemaCache.get_table(@tenant_id, "public", "orders")
      assert "id" in table.primary_key
    end

    test "discovers relationships - M2O" do
      rels = SchemaCache.get_relationships(@tenant_id, "public", "orders")
      customer_rel = Enum.find(rels, &(&1.target_table == "customers"))

      assert customer_rel.cardinality == :m2o
      assert customer_rel.source_columns == ["customer_id"]
      assert customer_rel.target_columns == ["id"]
    end

    test "discovers relationships - O2M" do
      rels = SchemaCache.get_relationships(@tenant_id, "public", "customers")
      orders_rel = Enum.find(rels, &(&1.target_table == "orders"))

      assert orders_rel.cardinality == :o2m
    end

    test "discovers relationships - O2O" do
      rels = SchemaCache.get_relationships(@tenant_id, "public", "customers")
      profile_rel = Enum.find(rels, &(&1.target_table == "customer_profiles"))

      assert profile_rel.cardinality == :o2o
    end

    test "discovers relationships - M2M" do
      rels = SchemaCache.get_relationships(@tenant_id, "public", "posts")
      tags_rel = Enum.find(rels, &(&1.target_table == "tags"))

      assert tags_rel.cardinality == :m2m
      assert tags_rel.junction.table == "post_tags"
    end
  end

  describe "SQL generation with relations" do
    test "generates SQL for M2O embedding (orders with customer)" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql_with_relations(
          @tenant_id,
          "public",
          "orders",
          "select=id,status,customers(id,name)"
        )

      assert result.query =~ "LEFT JOIN LATERAL"
      assert result.query =~ "row_to_json"
      assert result.query =~ ~s("public"."customers")
    end

    test "generates SQL for O2M embedding (customers with orders)" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql_with_relations(
          @tenant_id,
          "public",
          "customers",
          "select=id,name,orders(id,status,total_amount)"
        )

      assert result.query =~ "LEFT JOIN LATERAL"
      assert result.query =~ "json_agg"
      assert result.query =~ ~s("public"."orders")
    end

    test "generates SQL for M2M embedding (posts with tags)" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql_with_relations(
          @tenant_id,
          "public",
          "posts",
          "select=id,title,tags(id,name)"
        )

      assert result.query =~ "LEFT JOIN LATERAL"
      assert result.query =~ "json_agg"
      assert result.query =~ ~s("public"."post_tags")
      assert result.query =~ ~s("public"."tags")
    end

    test "generates SQL for O2O embedding (customers with profile)" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql_with_relations(
          @tenant_id,
          "public",
          "customers",
          "select=id,name,customer_profiles(bio,avatar_url)"
        )

      assert result.query =~ "LEFT JOIN LATERAL"
      assert result.query =~ "row_to_json"
    end

    test "handles aliased relations" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql_with_relations(
          @tenant_id,
          "public",
          "orders",
          "select=id,buyer:customers(name)"
        )

      assert result.query =~ ~s(AS "buyer")
    end
  end

  describe "filter operations" do
    test "equality filter on text column", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&name=eq.Alice Johnson")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("customers", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) == 1
    end

    test "in filter with multiple values", %{conn: conn} do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=*&status=in.(completed,processing)")

      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("orders", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "greater than filter on numeric", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&price=gt.100")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "pattern matching with ilike", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&name=ilike.%laptop%")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "is null filter", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&notes=is.null")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("orders", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "boolean filter with is.true", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&published=is.true")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("posts", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "negated filter", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&status=not.eq.cancelled")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("orders", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "JSON field filter", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&metadata->>tier=eq.gold")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("customers", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end
  end

  describe "logic trees" do
    test "AND logic with multiple conditions", %{conn: conn} do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=*&and=(price.gte.50,price.lte.150)")

      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "OR logic for multiple status values", %{conn: conn} do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=*&or=(status.eq.pending,status.eq.processing)")

      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("orders", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "nested logic trees", %{conn: conn} do
      query_str = "select=*&and=(category.eq.Electronics,or(price.lt.100,stock.gt.100))"
      {:ok, params} = PostgrestParser.parse_query_string(query_str)
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert is_list(result.rows)
    end
  end

  describe "ordering and pagination" do
    test "order by single column descending", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,price&order=price.desc")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      prices = Enum.map(result.rows, fn [_id, price] -> price end)
      assert prices == Enum.sort(prices, :desc)
    end

    test "order by multiple columns", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&order=category.asc,price.desc")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) >= 1
    end

    test "pagination with limit and offset", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&order=id.asc&limit=3&offset=2")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) == 3
    end

    test "limit only", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*&limit=5")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("products", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(result.rows) == 5
    end
  end

  describe "column selection" do
    test "select specific columns", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name,email")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("customers", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(hd(result.rows)) == 3
    end

    test "select with alias", %{conn: conn} do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=customer_name:name,customer_email:email")

      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("customers", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(hd(result.rows)) == 2
    end

    test "select all with wildcard", %{conn: conn} do
      {:ok, params} = PostgrestParser.parse_query_string("select=*")
      {:ok, %{query: query, params: sql_params}} = PostgrestParser.to_sql("customers", params)

      {:ok, result} = Postgrex.query(conn, query, sql_params)
      assert length(hd(result.rows)) >= 4
    end
  end

  describe "error handling" do
    test "returns error for unknown relationship" do
      {:error, reason} =
        PostgrestParser.query_string_to_sql_with_relations(
          @tenant_id,
          "public",
          "customers",
          "select=id,nonexistent_relation(id)"
        )

      assert reason =~ "not found"
    end

    test "returns error for invalid operator" do
      {:error, reason} = PostgrestParser.parse_query_string("id=invalid.1")
      assert reason =~ "unknown operator"
    end

    test "returns error for invalid limit value" do
      {:error, reason} = PostgrestParser.parse_query_string("limit=abc")
      assert reason =~ "non-negative integer"
    end
  end
end
