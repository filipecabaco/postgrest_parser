defmodule PostgrestParserTest do
  use ExUnit.Case, async: true

  alias PostgrestParser
  alias PostgrestParser.AST.Field
  alias PostgrestParser.AST.Filter
  alias PostgrestParser.AST.LogicTree
  alias PostgrestParser.AST.OrderTerm
  alias PostgrestParser.AST.ParsedParams
  alias PostgrestParser.AST.SelectItem

  doctest PostgrestParser
  doctest PostgrestParser.FilterParser
  doctest PostgrestParser.SelectParser
  doctest PostgrestParser.OrderParser
  doctest PostgrestParser.LogicParser
  doctest PostgrestParser.SqlBuilder

  describe "parse_query_string/1" do
    test "parses empty query string" do
      assert {:ok, %ParsedParams{filters: [], select: nil, order: [], limit: nil, offset: nil}} =
               PostgrestParser.parse_query_string("")
    end

    test "parses simple filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq.1")

      assert [%Filter{operator: :eq, value: "1"}] = params.filters
      assert %Field{name: "id"} = hd(params.filters).field
    end

    test "parses multiple filters" do
      {:ok, params} = PostgrestParser.parse_query_string("id=gt.5&status=eq.active")

      assert length(params.filters) == 2
    end

    test "parses select with columns" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name,email")

      assert length(params.select) == 3
      assert Enum.map(params.select, & &1.name) == ["id", "name", "email"]
    end

    test "parses order clause" do
      {:ok, params} = PostgrestParser.parse_query_string("order=created_at.desc")

      assert [%OrderTerm{direction: :desc}] = params.order
      assert %Field{name: "created_at"} = hd(params.order).field
    end

    test "parses limit and offset" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=10&offset=20")

      assert params.limit == 10
      assert params.offset == 20
    end

    test "parses complex query with all components" do
      query = "select=id,name&status=eq.active&order=id.desc&limit=10&offset=5"
      {:ok, params} = PostgrestParser.parse_query_string(query)

      assert length(params.select) == 2
      assert length(params.filters) == 1
      assert length(params.order) == 1
      assert params.limit == 10
      assert params.offset == 5
    end
  end

  describe "filter operators" do
    test "parses equality filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.john")
      assert [%Filter{operator: :eq, value: "john"}] = params.filters
    end

    test "parses not equal filter" do
      {:ok, params} = PostgrestParser.parse_query_string("status=neq.deleted")
      assert [%Filter{operator: :neq, value: "deleted"}] = params.filters
    end

    test "parses greater than filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=gt.18")
      assert [%Filter{operator: :gt, value: "18"}] = params.filters
    end

    test "parses greater than or equal filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=gte.21")
      assert [%Filter{operator: :gte, value: "21"}] = params.filters
    end

    test "parses less than filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=lt.65")
      assert [%Filter{operator: :lt, value: "65"}] = params.filters
    end

    test "parses less than or equal filter" do
      {:ok, params} = PostgrestParser.parse_query_string("price=lte.100")
      assert [%Filter{operator: :lte, value: "100"}] = params.filters
    end

    test "parses like filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like.John%")
      assert [%Filter{operator: :like, value: "John%"}] = params.filters
    end

    test "parses ilike filter" do
      {:ok, params} = PostgrestParser.parse_query_string("email=ilike.%@example.com")
      assert [%Filter{operator: :ilike, value: "%@example.com"}] = params.filters
    end

    test "parses in filter with list" do
      {:ok, params} = PostgrestParser.parse_query_string("status=in.(active,pending,review)")

      assert [%Filter{operator: :in, value: ["active", "pending", "review"]}] = params.filters
    end

    test "parses is null filter" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=is.null")
      assert [%Filter{operator: :is, value: "null"}] = params.filters
    end

    test "parses is not null filter" do
      {:ok, params} = PostgrestParser.parse_query_string("email=is.not_null")
      assert [%Filter{operator: :is, value: "not_null"}] = params.filters
    end

    test "parses is true filter" do
      {:ok, params} = PostgrestParser.parse_query_string("active=is.true")
      assert [%Filter{operator: :is, value: "true"}] = params.filters
    end

    test "parses is false filter" do
      {:ok, params} = PostgrestParser.parse_query_string("verified=is.false")
      assert [%Filter{operator: :is, value: "false"}] = params.filters
    end

    test "parses full-text search filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=fts.postgres")
      assert [%Filter{operator: :fts, value: "postgres"}] = params.filters
    end

    test "parses negated filter" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.eq.deleted")
      assert [%Filter{operator: :eq, value: "deleted", negated?: true}] = params.filters
    end

    test "parses negated in filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=not.in.(1,2,3)")
      assert [%Filter{operator: :in, value: ["1", "2", "3"], negated?: true}] = params.filters
    end

    test "returns error for invalid operator" do
      assert {:error, _} = PostgrestParser.parse_query_string("id=invalid.1")
    end
  end

  describe "JSON path filters" do
    test "parses single arrow JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->name=eq.test")

      assert [%Filter{field: %Field{name: "data", json_path: [{:arrow, "name"}]}}] =
               params.filters
    end

    test "parses double arrow JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->>name=eq.test")

      assert [%Filter{field: %Field{name: "data", json_path: [{:double_arrow, "name"}]}}] =
               params.filters
    end

    test "parses nested JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->outer->>inner=eq.value")

      assert [
               %Filter{
                 field: %Field{
                   name: "data",
                   json_path: [{:arrow, "outer"}, {:double_arrow, "inner"}]
                 }
               }
             ] = params.filters
    end
  end

  describe "select parsing" do
    test "parses wildcard select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=*")
      assert [%SelectItem{type: :field, name: "*"}] = params.select
    end

    test "parses multiple columns" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name,email")

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{type: :field, name: "name"},
               %SelectItem{type: :field, name: "email"}
             ] = params.select
    end

    test "parses aliased column" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user_name:name")
      assert [%SelectItem{type: :field, name: "name", alias: "user_name"}] = params.select
    end

    test "parses relation with columns" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,posts(id,title)")

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{
                 type: :relation,
                 name: "posts",
                 children: [
                   %SelectItem{type: :field, name: "id"},
                   %SelectItem{type: :field, name: "title"}
                 ]
               }
             ] = params.select
    end

    test "parses spread relation" do
      {:ok, params} = PostgrestParser.parse_query_string("select=...profile(bio)")

      assert [
               %SelectItem{
                 type: :spread,
                 name: "profile",
                 children: [%SelectItem{type: :field, name: "bio"}]
               }
             ] = params.select
    end

    test "parses nested relations" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,author(name,posts(title))")

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{
                 type: :relation,
                 name: "author",
                 children: [
                   %SelectItem{type: :field, name: "name"},
                   %SelectItem{
                     type: :relation,
                     name: "posts",
                     children: [%SelectItem{type: :field, name: "title"}]
                   }
                 ]
               }
             ] = params.select
    end
  end

  describe "order parsing" do
    test "parses simple order" do
      {:ok, params} = PostgrestParser.parse_query_string("order=id")
      assert [%OrderTerm{direction: :asc, nulls: nil}] = params.order
    end

    test "parses descending order" do
      {:ok, params} = PostgrestParser.parse_query_string("order=created_at.desc")
      assert [%OrderTerm{direction: :desc}] = params.order
    end

    test "parses order with nulls first" do
      {:ok, params} = PostgrestParser.parse_query_string("order=priority.desc.nullsfirst")
      assert [%OrderTerm{direction: :desc, nulls: :first}] = params.order
    end

    test "parses order with nulls last" do
      {:ok, params} = PostgrestParser.parse_query_string("order=updated_at.asc.nullslast")
      assert [%OrderTerm{direction: :asc, nulls: :last}] = params.order
    end

    test "parses multiple order terms" do
      {:ok, params} = PostgrestParser.parse_query_string("order=status.desc,created_at.asc")

      assert [
               %OrderTerm{direction: :desc},
               %OrderTerm{direction: :asc}
             ] = params.order
    end
  end

  describe "logic trees" do
    test "parses and logic" do
      {:ok, params} = PostgrestParser.parse_query_string("and=(id.eq.1,name.eq.john)")

      assert [%LogicTree{operator: :and, negated?: false, conditions: conditions}] =
               params.filters

      assert length(conditions) == 2
    end

    test "parses or logic" do
      {:ok, params} =
        PostgrestParser.parse_query_string("or=(status.eq.active,status.eq.pending)")

      assert [%LogicTree{operator: :or, negated?: false, conditions: conditions}] = params.filters
      assert length(conditions) == 2
    end

    test "parses negated and logic" do
      {:ok, params} =
        PostgrestParser.parse_query_string("not.and=(deleted.eq.true,archived.eq.true)")

      assert [%LogicTree{operator: :and, negated?: true}] = params.filters
    end

    test "parses nested logic" do
      {:ok, params} =
        PostgrestParser.parse_query_string("and=(status.eq.active,or(type.eq.a,type.eq.b))")

      assert [%LogicTree{operator: :and, conditions: [_, %LogicTree{operator: :or}]}] =
               params.filters
    end
  end

  describe "to_sql/2" do
    test "generates simple select query" do
      {:ok, params} = PostgrestParser.parse_query_string("select=*")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.query == ~s(SELECT * FROM "users")
      assert result.params == []
    end

    test "generates query with filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq.1")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.query == ~s(SELECT * FROM "users" WHERE "id" = $1)
      assert result.params == [1]
    end

    test "generates query with multiple filters" do
      {:ok, params} = PostgrestParser.parse_query_string("status=eq.active&age=gt.18")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "WHERE")
      assert String.contains?(result.query, "AND")
      assert length(result.params) == 2
    end

    test "generates query with in filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=in.(1,2,3)")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "= ANY($1)")
      assert result.params == [[1, 2, 3]]
    end

    test "generates query with is null filter" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=is.null")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "IS NULL")
      assert result.params == []
    end

    test "generates query with order" do
      {:ok, params} = PostgrestParser.parse_query_string("order=id.desc")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, ~s(ORDER BY "id" DESC))
    end

    test "generates query with order and nulls" do
      {:ok, params} = PostgrestParser.parse_query_string("order=priority.desc.nullsfirst")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "DESC NULLS FIRST")
    end

    test "generates query with limit" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=10")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "LIMIT $1")
      assert result.params == [10]
    end

    test "generates query with offset" do
      {:ok, params} = PostgrestParser.parse_query_string("offset=20")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "OFFSET $1")
      assert result.params == [20]
    end

    test "generates query with limit and offset" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=10&offset=20")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "LIMIT $1 OFFSET $2")
      assert result.params == [10, 20]
    end

    test "generates query with selected columns" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.query == ~s(SELECT "id", "name" FROM "users")
    end

    test "generates query with aliased column" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user_name:name")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.query == ~s(SELECT "name" AS "user_name" FROM "users")
    end

    test "generates query with JSON path in filter" do
      {:ok, params} = PostgrestParser.parse_query_string("data->>name=eq.test")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, ~s("data"->>'name'))
    end

    test "generates query with logic tree" do
      {:ok, params} = PostgrestParser.parse_query_string("or=(id.eq.1,id.eq.2)")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, " OR ")
      assert String.contains?(result.query, "(")
    end

    test "generates query with negated logic tree" do
      {:ok, params} =
        PostgrestParser.parse_query_string("not.and=(deleted.eq.true,archived.eq.true)")

      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "NOT (")
    end
  end

  describe "query_string_to_sql/2" do
    test "parses and generates SQL in one step" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql("users", "select=*&id=eq.1&order=id.desc")

      assert String.contains?(result.query, "SELECT *")
      assert String.contains?(result.query, ~s(FROM "users"))
      assert String.contains?(result.query, "WHERE")
      assert String.contains?(result.query, "ORDER BY")
    end
  end

  describe "build_filter_clause/1" do
    test "builds filter clause from params" do
      {:ok, result} =
        PostgrestParser.build_filter_clause(%{"id" => "eq.1", "status" => "eq.active"})

      assert String.contains?(result.clause, "$1")
      assert String.contains?(result.clause, "$2")
      assert length(result.params) == 2
    end

    test "returns empty clause for no filters" do
      {:ok, result} = PostgrestParser.build_filter_clause(%{"select" => "*"})

      assert result.clause == ""
      assert result.params == []
    end
  end

  describe "SQL injection prevention" do
    test "escapes double quotes in identifiers" do
      {:ok, result} = PostgrestParser.query_string_to_sql(~s(users"--), "select=*")
      assert String.contains?(result.query, ~s("users""--"))
    end

    test "uses parameterized queries for values" do
      {:ok, result} =
        PostgrestParser.query_string_to_sql("users", "name=eq.'; DROP TABLE users;--")

      assert String.contains?(result.query, "$1")
      assert "'; DROP TABLE users;--" in result.params
      refute String.contains?(result.query, "DROP TABLE")
    end

    test "escapes filter values through parameterization" do
      malicious_value = "1; DELETE FROM users WHERE 1=1;--"
      {:ok, result} = PostgrestParser.query_string_to_sql("users", "id=eq.#{malicious_value}")

      assert result.params == [malicious_value]
      refute String.contains?(result.query, "DELETE")
    end
  end

  describe "edge cases" do
    test "handles URL encoded characters" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.John%20Doe")
      assert [%Filter{value: "John Doe"}] = params.filters
    end

    test "handles empty filter value" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.")
      assert [%Filter{value: ""}] = params.filters
    end

    test "returns error for invalid limit" do
      assert {:error, _} = PostgrestParser.parse_query_string("limit=abc")
    end

    test "returns error for negative limit" do
      assert {:error, _} = PostgrestParser.parse_query_string("limit=-1")
    end

    test "returns error for invalid offset" do
      assert {:error, _} = PostgrestParser.parse_query_string("offset=xyz")
    end
  end

  describe "RelationBuilder" do
    alias PostgrestParser.RelationBuilder
    alias PostgrestParser.SchemaCache.Relationship

    test "builds LATERAL JOIN for one-to-many relationship" do
      select_item = %SelectItem{
        type: :relation,
        name: "orders",
        children: [
          %SelectItem{type: :field, name: "id"},
          %SelectItem{type: :field, name: "total"}
        ]
      }

      relationship = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        source_schema: "public",
        source_table: "customers",
        source_columns: ["id"],
        target_schema: "public",
        target_table: "orders",
        target_columns: ["customer_id"],
        cardinality: :o2m,
        junction: nil
      }

      {join_sql, select_sql} =
        RelationBuilder.build_single_relation_join(select_item, relationship, "customers", 0)

      assert join_sql =~ "LEFT JOIN LATERAL"
      assert join_sql =~ "json_agg"
      assert join_sql =~ ~s("public"."orders")
      assert join_sql =~ ~s("customers"."id" = "orders_0"."customer_id")
      assert select_sql =~ ~s(orders_0_agg.orders_0 AS "orders")
    end

    test "builds LATERAL JOIN for many-to-one relationship" do
      select_item = %SelectItem{
        type: :relation,
        name: "customer",
        children: [
          %SelectItem{type: :field, name: "id"},
          %SelectItem{type: :field, name: "name"}
        ]
      }

      relationship = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        source_schema: "public",
        source_table: "orders",
        source_columns: ["customer_id"],
        target_schema: "public",
        target_table: "customers",
        target_columns: ["id"],
        cardinality: :m2o,
        junction: nil
      }

      {join_sql, select_sql} =
        RelationBuilder.build_single_relation_join(select_item, relationship, "orders", 0)

      assert join_sql =~ "LEFT JOIN LATERAL"
      assert join_sql =~ "row_to_json"
      assert join_sql =~ "LIMIT 1"
      assert join_sql =~ ~s("public"."customers")
      assert join_sql =~ ~s("orders"."customer_id" = "customer_0"."id")
      assert select_sql =~ ~s(customer_0_agg.customer_0 AS "customer")
    end

    test "builds LATERAL JOIN for many-to-many relationship" do
      select_item = %SelectItem{
        type: :relation,
        name: "tags",
        children: [
          %SelectItem{type: :field, name: "id"},
          %SelectItem{type: :field, name: "name"}
        ]
      }

      relationship = %Relationship{
        constraint_name: "post_tags_post_fkey_post_tags_tag_fkey",
        source_schema: "public",
        source_table: "posts",
        source_columns: ["id"],
        target_schema: "public",
        target_table: "tags",
        target_columns: ["id"],
        cardinality: :m2m,
        junction: %{
          schema: "public",
          table: "post_tags",
          source_constraint: "post_tags_post_fkey",
          source_columns: ["post_id"],
          target_constraint: "post_tags_tag_fkey",
          target_columns: ["tag_id"]
        }
      }

      {join_sql, select_sql} =
        RelationBuilder.build_m2m_join(select_item, relationship, "posts", 0)

      assert join_sql =~ "LEFT JOIN LATERAL"
      assert join_sql =~ "json_agg"
      assert join_sql =~ ~s("public"."post_tags")
      assert join_sql =~ ~s("public"."tags")
      assert join_sql =~ ~s("posts"."id" = "junction_0"."post_id")
      assert join_sql =~ ~s("junction_0"."tag_id" = "tags_0"."id")
      assert select_sql =~ ~s(tags_0_agg.tags_0 AS "tags")
    end
  end

  describe "SchemaCache.Relationship struct" do
    alias PostgrestParser.SchemaCache.Relationship

    test "creates M2O relationship struct" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        source_schema: "public",
        source_table: "orders",
        source_columns: ["customer_id"],
        target_schema: "public",
        target_table: "customers",
        target_columns: ["id"],
        cardinality: :m2o,
        junction: nil
      }

      assert rel.cardinality == :m2o
      assert rel.source_table == "orders"
      assert rel.target_table == "customers"
    end

    test "creates M2M relationship with junction" do
      rel = %Relationship{
        constraint_name: "posts_tags_m2m",
        source_schema: "public",
        source_table: "posts",
        source_columns: ["id"],
        target_schema: "public",
        target_table: "tags",
        target_columns: ["id"],
        cardinality: :m2m,
        junction: %{
          schema: "public",
          table: "post_tags",
          source_constraint: "post_tags_post_id_fkey",
          source_columns: ["post_id"],
          target_constraint: "post_tags_tag_id_fkey",
          target_columns: ["tag_id"]
        }
      }

      assert rel.cardinality == :m2m
      assert rel.junction.table == "post_tags"
      assert rel.junction.source_columns == ["post_id"]
      assert rel.junction.target_columns == ["tag_id"]
    end
  end
end
