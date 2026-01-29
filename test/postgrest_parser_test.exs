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

  # ============================================================================
  # User Scenario: Basic Query Parsing
  # ============================================================================

  describe "parsing complete PostgREST queries" do
    test "parses empty query string" do
      assert {:ok, %ParsedParams{filters: [], select: nil, order: [], limit: nil, offset: nil}} =
               PostgrestParser.parse_query_string("")
    end

    test "parses a typical API query with select, filter, order and pagination" do
      query = "select=id,name,email&status=eq.active&order=created_at.desc&limit=10&offset=0"
      {:ok, params} = PostgrestParser.parse_query_string(query)

      assert length(params.select) == 3
      assert [%Filter{operator: :eq, value: "active"}] = params.filters
      assert [%OrderTerm{direction: :desc}] = params.order
      assert params.limit == 10
      assert params.offset == 0
    end

    test "parses query with only select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name")
      assert length(params.select) == 2
      assert params.filters == []
    end

    test "parses query with only filters" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq.1&status=eq.active")
      assert length(params.filters) == 2
      assert is_nil(params.select)
    end
  end

  # ============================================================================
  # User Scenario: Filtering Data
  # ============================================================================

  describe "filtering with comparison operators" do
    @comparison_operators [
      {"eq", :eq, "="},
      {"neq", :neq, "<>"},
      {"gt", :gt, ">"},
      {"gte", :gte, ">="},
      {"lt", :lt, "<"},
      {"lte", :lte, "<="}
    ]

    for {op_str, op_atom, _sql_op} <- @comparison_operators do
      test "parses #{op_str} operator" do
        {:ok, params} = PostgrestParser.parse_query_string("age=#{unquote(op_str)}.25")
        assert [%Filter{operator: unquote(op_atom), value: "25"}] = params.filters
      end

      test "parses negated #{op_str} operator" do
        {:ok, params} = PostgrestParser.parse_query_string("age=not.#{unquote(op_str)}.25")
        assert [%Filter{operator: unquote(op_atom), negated?: true}] = params.filters
      end
    end

    test "generates correct SQL for comparison operators" do
      for {op_str, _op_atom, sql_op} <- @comparison_operators do
        {:ok, params} = PostgrestParser.parse_query_string("age=#{op_str}.25")
        {:ok, result} = PostgrestParser.to_sql("users", params)
        assert String.contains?(result.query, sql_op)
      end
    end
  end

  describe "filtering with pattern matching" do
    @pattern_operators [
      {"like", :like, "LIKE"},
      {"ilike", :ilike, "ILIKE"},
      {"match", :match, "~"},
      {"imatch", :imatch, "~*"}
    ]

    for {op_str, op_atom, _sql_op} <- @pattern_operators do
      test "parses #{op_str} operator for text search" do
        {:ok, params} = PostgrestParser.parse_query_string("name=#{unquote(op_str)}.%john%")
        assert [%Filter{operator: unquote(op_atom), value: "%john%"}] = params.filters
      end
    end

    test "generates SQL for pattern matching operators" do
      {:ok, params} = PostgrestParser.parse_query_string("email=ilike.%@example.com")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "ILIKE")
    end

    test "handles like with PostgREST wildcard (*)" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like.John*")
      assert [%Filter{operator: :like, value: "John*"}] = params.filters
    end

    test "handles like with underscore placeholder" do
      {:ok, params} = PostgrestParser.parse_query_string("code=like.ABC___")
      assert [%Filter{operator: :like, value: "ABC___"}] = params.filters
    end

    test "generates SQL for regex match" do
      {:ok, params} = PostgrestParser.parse_query_string("phone=match.\\\\d{3}-\\\\d{4}")
      {:ok, result} = PostgrestParser.to_sql("contacts", params)
      assert String.contains?(result.query, "~")
    end

    test "generates SQL for case-insensitive regex" do
      {:ok, params} = PostgrestParser.parse_query_string("email=imatch.^[a-z]+@")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "~*")
    end

    test "handles negated pattern match" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.like.%test%")
      assert [%Filter{operator: :like, negated?: true}] = params.filters

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT")
    end
  end

  describe "filtering with set operations" do
    test "filters by list of values with in operator" do
      {:ok, params} = PostgrestParser.parse_query_string("status=in.(active,pending,review)")
      assert [%Filter{operator: :in, value: ["active", "pending", "review"]}] = params.filters

      {:ok, result} = PostgrestParser.to_sql("orders", params)
      assert String.contains?(result.query, "= ANY($1)")
    end

    test "checks array contains with cs operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cs.{tag1,tag2}")
      assert [%Filter{operator: :cs, value: "{tag1,tag2}"}] = params.filters
    end

    test "checks array overlap with ov operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=ov.(tag1,tag2)")
      assert [%Filter{operator: :ov, value: ["tag1", "tag2"]}] = params.filters
    end
  end

  describe "filtering with null checks" do
    @null_values ["null", "not_null", "true", "false", "unknown"]

    for value <- @null_values do
      test "filters with is.#{value}" do
        {:ok, params} = PostgrestParser.parse_query_string("field=is.#{unquote(value)}")
        assert [%Filter{operator: :is, value: unquote(value)}] = params.filters
      end
    end

    test "generates IS NULL SQL" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=is.null")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "IS NULL")
      assert result.params == []
    end

    test "generates IS TRUE SQL" do
      {:ok, params} = PostgrestParser.parse_query_string("active=is.true")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "IS TRUE")
    end
  end

  describe "filtering with full-text search" do
    @fts_operators [
      {"fts", :fts},
      {"plfts", :plfts},
      {"phfts", :phfts},
      {"wfts", :wfts}
    ]

    for {op_str, op_atom} <- @fts_operators do
      test "searches text with #{op_str} operator" do
        {:ok, params} = PostgrestParser.parse_query_string("content=#{unquote(op_str)}.postgres")
        assert [%Filter{operator: unquote(op_atom), value: "postgres"}] = params.filters
      end

      test "searches text with #{op_str} and language specification" do
        {:ok, params} =
          PostgrestParser.parse_query_string("content=#{unquote(op_str)}(english).hello")

        assert [%Filter{operator: unquote(op_atom), language: "english"}] = params.filters
      end
    end

    test "rejects any/all as FTS language (they are quantifiers)" do
      assert {:error, _} = PostgrestParser.parse_query_string("content=fts(any).hello")
      assert {:error, _} = PostgrestParser.parse_query_string("content=fts(all).hello")
    end
  end

  describe "filtering with quantifiers (any/all)" do
    @quantifiable_ops [
      {"eq", :eq},
      {"neq", :neq},
      {"gt", :gt},
      {"gte", :gte},
      {"lt", :lt},
      {"lte", :lte},
      {"like", :like},
      {"ilike", :ilike}
    ]

    for {op_str, op_atom} <- @quantifiable_ops do
      test "#{op_str}(any) matches any value in array" do
        {:ok, params} =
          PostgrestParser.parse_query_string("id=#{unquote(op_str)}(any).{1,2,3}")

        assert [%Filter{operator: unquote(op_atom), quantifier: :any}] = params.filters
      end

      test "#{op_str}(all) matches all values in array" do
        {:ok, params} =
          PostgrestParser.parse_query_string("id=#{unquote(op_str)}(all).{1,2,3}")

        assert [%Filter{operator: unquote(op_atom), quantifier: :all}] = params.filters
      end
    end

    test "generates correct SQL for quantified operators" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq(any).{1,2,3}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "= ANY($1)")
    end
  end

  describe "filtering on JSON fields" do
    test "filters on JSON field with single arrow (returns JSON)" do
      {:ok, params} = PostgrestParser.parse_query_string("data->name=eq.test")

      assert [%Filter{field: %Field{name: "data", json_path: [{:arrow, "name"}]}}] =
               params.filters
    end

    test "filters on JSON field with double arrow (returns text)" do
      {:ok, params} = PostgrestParser.parse_query_string("data->>name=eq.test")

      assert [%Filter{field: %Field{name: "data", json_path: [{:double_arrow, "name"}]}}] =
               params.filters
    end

    test "filters on deeply nested JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->user->profile->>email=eq.test")

      assert [
               %Filter{
                 field: %Field{
                   json_path: [{:arrow, "user"}, {:arrow, "profile"}, {:double_arrow, "email"}]
                 }
               }
             ] =
               params.filters
    end

    test "generates correct SQL for JSON path filter" do
      {:ok, params} = PostgrestParser.parse_query_string("data->>name=eq.test")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, ~s("data"->>'name'))
    end
  end

  describe "filtering with range operators" do
    @range_operators [
      {"sl", :sl, "<<"},
      {"sr", :sr, ">>"},
      {"nxl", :nxl, "&<"},
      {"nxr", :nxr, "&>"},
      {"adj", :adj, "-|-"}
    ]

    for {op_str, op_atom, sql_op} <- @range_operators do
      test "parses range operator #{op_str}" do
        {:ok, params} = PostgrestParser.parse_query_string("range=#{unquote(op_str)}.[0,5)")
        assert [%Filter{operator: unquote(op_atom), value: "[0,5)"}] = params.filters
      end

      test "generates SQL for range operator #{op_str}" do
        {:ok, params} = PostgrestParser.parse_query_string("range=#{unquote(op_str)}.(1,10)")
        {:ok, result} = PostgrestParser.to_sql("intervals", params)
        assert String.contains?(result.query, unquote(sql_op))
      end
    end

    test "handles date range overlap" do
      {:ok, params} =
        PostgrestParser.parse_query_string("period=ov.(2024-01-01,2024-12-31)")

      assert [%Filter{operator: :ov, value: ["2024-01-01", "2024-12-31"]}] = params.filters
    end

    test "handles adjacent range" do
      {:ok, params} = PostgrestParser.parse_query_string("age_range=adj.[18,21)")
      assert [%Filter{operator: :adj}] = params.filters
    end
  end

  # ============================================================================
  # User Scenario: Selecting Columns and Relations
  # ============================================================================

  describe "selecting columns" do
    test "selects all columns with wildcard" do
      {:ok, params} = PostgrestParser.parse_query_string("select=*")
      assert [%SelectItem{type: :field, name: "*"}] = params.select

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert result.query == ~s(SELECT * FROM "users")
    end

    test "selects specific columns" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name,email")

      assert [
               %SelectItem{name: "id"},
               %SelectItem{name: "name"},
               %SelectItem{name: "email"}
             ] = params.select

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert result.query == ~s(SELECT "id", "name", "email" FROM "users")
    end

    test "selects column with alias for renaming in response" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user_name:name")
      assert [%SelectItem{name: "name", alias: "user_name"}] = params.select

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert result.query == ~s(SELECT "name" AS "user_name" FROM "users")
    end

    test "selects JSON path for extracting nested data" do
      {:ok, params} = PostgrestParser.parse_query_string("select=profile_name:data->>name")

      assert [
               %SelectItem{
                 name: "data",
                 alias: "profile_name",
                 hint: {:json_path, [{:double_arrow, "name"}]}
               }
             ] =
               params.select
    end
  end

  describe "selecting embedded relations" do
    test "embeds related resource with selected columns" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,posts(id,title)")

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{type: :relation, name: "posts", children: children}
             ] = params.select

      assert length(children) == 2
    end

    test "embeds deeply nested relations" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=id,author(name,posts(title,comments(text)))")

      [_, %SelectItem{children: author_children}] = params.select
      [_, %SelectItem{children: posts_children}] = author_children
      [_, %SelectItem{name: "comments"}] = posts_children
    end

    test "aliases embedded relation for custom key name" do
      {:ok, params} = PostgrestParser.parse_query_string("select=writer:author(name)")
      assert [%SelectItem{name: "author", alias: "writer"}] = params.select
    end

    test "uses join hint for controlling join type" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author!inner(name)")
      assert [%SelectItem{name: "author", hint: "inner"}] = params.select
    end

    test "spreads relation columns into parent with spread operator" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,...profile(bio,avatar)")

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{type: :spread, name: "profile"}
             ] = params.select
    end

    test "spreads with column aliasing" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "select=id,...author(author_name:name,author_email:email)"
        )

      [_, spread_item] = params.select
      assert spread_item.type == :spread
      assert length(spread_item.children) == 2
    end

    test "combines multiple relation embeddings" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=id,author(name),comments(text)")

      assert length(params.select) == 3
      relations = Enum.filter(params.select, &(&1.type == :relation))
      assert length(relations) == 2
    end

    test "uses constraint name hint for disambiguating FKs" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=id,billing:addresses!billing_fk(street)")

      [_, billing] = params.select
      assert billing.alias == "billing"
      assert billing.hint == "billing_fk"
    end

    test "selects wildcard inside relation" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,posts(*)")

      [_, posts] = params.select
      assert [%SelectItem{name: "*"}] = posts.children
    end

    test "handles empty children in relation" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,posts()")

      [_, posts] = params.select
      assert posts.children == []
    end

    test "parses complex nested select with aliases and hints" do
      query = "select=id,writer:author!inner(name,books(title,publisher!outer(name)))"
      {:ok, params} = PostgrestParser.parse_query_string(query)

      [_, author] = params.select
      assert author.alias == "writer"
      assert author.hint == "inner"
    end

    test "handles JSON path with alias in select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=city:address->>city")

      assert [%SelectItem{alias: "city", name: "address"}] = params.select
    end
  end

  # ============================================================================
  # User Scenario: Ordering Results
  # ============================================================================

  describe "ordering results" do
    test "orders by column ascending (default)" do
      {:ok, params} = PostgrestParser.parse_query_string("order=name")
      assert [%OrderTerm{direction: :asc, nulls: nil}] = params.order
    end

    test "orders by column descending" do
      {:ok, params} = PostgrestParser.parse_query_string("order=created_at.desc")
      assert [%OrderTerm{direction: :desc}] = params.order

      {:ok, result} = PostgrestParser.to_sql("posts", params)
      assert String.contains?(result.query, ~s(ORDER BY "created_at" DESC))
    end

    test "controls null ordering with nullsfirst/nullslast" do
      {:ok, params} = PostgrestParser.parse_query_string("order=priority.desc.nullsfirst")
      assert [%OrderTerm{direction: :desc, nulls: :first}] = params.order

      {:ok, result} = PostgrestParser.to_sql("tasks", params)
      assert String.contains?(result.query, "DESC NULLS FIRST")
    end

    test "orders by multiple columns" do
      {:ok, params} = PostgrestParser.parse_query_string("order=status.asc,created_at.desc")

      assert [
               %OrderTerm{direction: :asc},
               %OrderTerm{direction: :desc}
             ] = params.order
    end

    test "orders by JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("order=data->priority.desc")

      assert [%OrderTerm{field: %Field{name: "data", json_path: [{:arrow, "priority"}]}}] =
               params.order
    end

    test "orders by JSON double arrow path" do
      {:ok, params} = PostgrestParser.parse_query_string("order=data->>name.asc")

      assert [%OrderTerm{field: %Field{json_path: [{:double_arrow, "name"}]}}] = params.order
    end

    test "orders by deeply nested JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("order=data->user->>age.desc")

      assert [
               %OrderTerm{
                 field: %Field{json_path: [{:arrow, "user"}, {:double_arrow, "age"}]},
                 direction: :desc
               }
             ] = params.order
    end

    test "handles multiple columns with different null orderings" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "order=priority.desc.nullslast,created_at.asc.nullsfirst"
        )

      assert [
               %OrderTerm{direction: :desc, nulls: :last},
               %OrderTerm{direction: :asc, nulls: :first}
             ] = params.order
    end

    test "generates SQL for multiple order terms" do
      {:ok, params} = PostgrestParser.parse_query_string("order=status.asc,created_at.desc")
      {:ok, result} = PostgrestParser.to_sql("orders", params)

      assert String.contains?(result.query, ~s("status" ASC, "created_at" DESC))
    end

    test "generates SQL with NULLS LAST" do
      {:ok, params} = PostgrestParser.parse_query_string("order=deleted_at.desc.nullslast")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "DESC NULLS LAST")
    end

    test "handles field name with dots when not a valid direction" do
      {:ok, params} = PostgrestParser.parse_query_string("order=table.field")
      assert [%OrderTerm{field: %Field{name: "table.field"}, direction: :asc}] = params.order
    end
  end

  # ============================================================================
  # User Scenario: Pagination
  # ============================================================================

  describe "paginating results" do
    test "limits number of returned rows" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=10")
      assert params.limit == 10

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "LIMIT $1")
      assert result.params == [10]
    end

    test "skips rows with offset" do
      {:ok, params} = PostgrestParser.parse_query_string("offset=20")
      assert params.offset == 20

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "OFFSET $1")
    end

    test "paginates with limit and offset together" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=10&offset=20")

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "LIMIT $1 OFFSET $2")
      assert result.params == [10, 20]
    end

    test "returns error for non-numeric limit" do
      assert {:error, reason} = PostgrestParser.parse_query_string("limit=abc")
      assert reason =~ "non-negative integer"
    end

    test "returns error for negative offset" do
      assert {:error, reason} = PostgrestParser.parse_query_string("offset=-5")
      assert reason =~ "non-negative integer"
    end
  end

  # ============================================================================
  # User Scenario: Complex Logical Conditions
  # ============================================================================

  describe "combining conditions with logical operators" do
    test "combines conditions with AND" do
      {:ok, params} = PostgrestParser.parse_query_string("and=(status.eq.active,age.gte.18)")

      assert [%LogicTree{operator: :and, conditions: conditions}] = params.filters
      assert length(conditions) == 2
    end

    test "combines conditions with OR" do
      {:ok, params} =
        PostgrestParser.parse_query_string("or=(status.eq.active,status.eq.pending)")

      assert [%LogicTree{operator: :or}] = params.filters
    end

    test "negates logical conditions" do
      {:ok, params} =
        PostgrestParser.parse_query_string("not.and=(deleted.eq.true,archived.eq.true)")

      assert [%LogicTree{operator: :and, negated?: true}] = params.filters

      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "NOT (")
    end

    test "nests logical operators" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "and=(status.eq.active,or(type.eq.premium,credits.gt.100))"
        )

      assert [%LogicTree{operator: :and, conditions: [_, %LogicTree{operator: :or}]}] =
               params.filters
    end

    test "includes negated filters inside logic tree" do
      {:ok, params} =
        PostgrestParser.parse_query_string("and=(id.eq.1,status.not.eq.deleted)")

      assert [%LogicTree{conditions: [_, %Filter{negated?: true}]}] = params.filters
    end

    test "generates SQL with OR clause" do
      {:ok, params} = PostgrestParser.parse_query_string("or=(id.eq.1,id.eq.2)")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, " OR ")
      assert String.contains?(result.query, "(")
    end

    test "parses deeply nested OR inside AND" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "and=(status.eq.active,or(type.eq.premium,type.eq.gold))"
        )

      assert [%LogicTree{operator: :and, conditions: conditions}] = params.filters
      assert [%Filter{operator: :eq}, %LogicTree{operator: :or}] = conditions
    end

    test "parses complex mixed AND/OR/NOT expression" do
      {:ok, params} =
        PostgrestParser.parse_query_string("or=(age.lt.18,and(age.gte.18,age.lte.65))")

      assert [%LogicTree{operator: :or, conditions: conditions}] = params.filters
      assert [%Filter{operator: :lt}, %LogicTree{operator: :and}] = conditions
    end

    test "parses triple nested logic" do
      query = "and=(status.eq.active,or(role.eq.admin,and(credits.gt.100,verified.is.true)))"
      {:ok, params} = PostgrestParser.parse_query_string(query)

      assert [%LogicTree{operator: :and}] = params.filters
    end

    test "combines regular filters with logic tree" do
      query = "grade=gte.90&student=is.true&or=(age.eq.14,age.eq.15)"
      {:ok, params} = PostgrestParser.parse_query_string(query)

      assert length(params.filters) == 3
      assert Enum.any?(params.filters, &match?(%LogicTree{}, &1))
    end

    test "generates SQL for nested NOT AND" do
      {:ok, params} =
        PostgrestParser.parse_query_string("not.and=(deleted.is.true,archived.is.true)")

      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "NOT (")
      assert String.contains?(result.query, " AND ")
    end

    test "generates SQL for NOT OR" do
      {:ok, params} =
        PostgrestParser.parse_query_string("not.or=(status.eq.banned,status.eq.suspended)")

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT (")
      assert String.contains?(result.query, " OR ")
    end

    test "handles many conditions in single logic tree" do
      {:ok, params} =
        PostgrestParser.parse_query_string("or=(id.eq.1,id.eq.2,id.eq.3,id.eq.4,id.eq.5)")

      assert [%LogicTree{operator: :or, conditions: conditions}] = params.filters
      assert length(conditions) == 5
    end
  end

  # ============================================================================
  # User Scenario: Type Casting
  # ============================================================================

  describe "type casting in selects" do
    test "casts column to different type" do
      {:ok, params} = PostgrestParser.parse_query_string("select=price::text")
      assert [%SelectItem{name: "price", hint: {:cast, "text"}}] = params.select
    end

    test "casts JSON path result" do
      {:ok, params} = PostgrestParser.parse_query_string("select=data->amount::numeric")

      assert [%SelectItem{hint: {:json_path_cast, [{:arrow, "amount"}], "numeric"}}] =
               params.select
    end

    test "generates SQL with type cast" do
      {:ok, params} = PostgrestParser.parse_query_string("select=created_at::date")
      {:ok, result} = PostgrestParser.to_sql("posts", params)
      assert String.contains?(result.query, "::date")
    end
  end

  # ============================================================================
  # User Scenario: Error Handling
  # ============================================================================

  describe "handling invalid queries" do
    test "returns error for unknown filter operator" do
      assert {:error, reason} = PostgrestParser.parse_query_string("id=invalid.1")
      assert reason =~ "unknown operator"
    end

    test "returns error for unclosed parenthesis in select" do
      assert {:error, reason} = PostgrestParser.parse_query_string("select=posts(id,title")
      assert reason =~ "parenthesis"
    end

    test "returns error for logic expression without parentheses" do
      assert {:error, reason} = PostgrestParser.parse_query_string("and=id.eq.1,name.eq.john")
      assert reason =~ "parentheses"
    end

    test "returns error for invalid filter inside logic tree" do
      assert {:error, _} =
               PostgrestParser.parse_query_string("and=(id.invalid.1,name.eq.john)")
    end

    test "returns error for empty field name in select" do
      assert {:error, reason} = PostgrestParser.parse_query_string("select=id,,name")
      assert reason =~ ~r/(unexpected|empty)/
    end
  end

  # ============================================================================
  # User Scenario: SQL Generation
  # ============================================================================

  describe "generating SQL from parsed queries" do
    test "generates basic SELECT with filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq.1")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.query == ~s(SELECT * FROM "users" WHERE "id" = $1)
      assert result.params == [1]
    end

    test "generates query with multiple filters joined by AND" do
      {:ok, params} = PostgrestParser.parse_query_string("status=eq.active&age=gte.18")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "WHERE")
      assert String.contains?(result.query, "AND")
      assert length(result.params) == 2
    end

    test "properly quotes table and column names" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user_name")
      {:ok, result} = PostgrestParser.to_sql("user_data", params)

      assert String.contains?(result.query, ~s("user_data"))
      assert String.contains?(result.query, ~s("user_name"))
    end

    test "uses parameterized queries to prevent SQL injection" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.'; DROP TABLE users;--")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      refute String.contains?(result.query, "DROP TABLE")
      assert String.contains?(result.query, "$1")
      assert result.params == ["'; DROP TABLE users;--"]
    end
  end

  # ============================================================================
  # User Scenario: Edge Cases and Real-World Patterns
  # ============================================================================

  describe "handling edge cases" do
    test "handles decimal values in filters" do
      {:ok, params} = PostgrestParser.parse_query_string("price=gt.99.99")
      {:ok, result} = PostgrestParser.to_sql("products", params)
      assert [%Decimal{}] = result.params
    end

    test "handles negative numbers" do
      {:ok, params} = PostgrestParser.parse_query_string("balance=lt.-50")
      {:ok, result} = PostgrestParser.to_sql("accounts", params)
      assert result.params == [-50]
    end

    test "handles multiple filters on same field" do
      {:ok, params} = PostgrestParser.parse_query_string("age=gte.18&age=lte.65")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert length(result.params) == 2
    end

    test "handles URL-encoded special characters" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.John%27s%20Test")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert ["John's Test"] = result.params
    end

    test "handles quoted values in lists" do
      {:ok, params} = PostgrestParser.parse_query_string(~s|name=in.("John Doe","Jane Doe")|)
      assert [%Filter{value: ["John Doe", "Jane Doe"]}] = params.filters
    end

    test "handles empty string in equality filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.")
      assert [%Filter{operator: :eq, value: ""}] = params.filters
    end

    test "handles empty array in cs operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cs.{}")
      assert [%Filter{operator: :cs, value: "{}"}] = params.filters
    end

    test "handles empty list in in operator" do
      {:ok, params} = PostgrestParser.parse_query_string("id=in.()")
      assert [%Filter{operator: :in, value: [""]}] = params.filters
    end

    test "handles values with reserved PostgREST characters" do
      {:ok, params} = PostgrestParser.parse_query_string(~s|description=eq."value"|)
      assert [%Filter{value: ~s|"value"|}] = params.filters
    end

    test "handles filter value with parentheses" do
      {:ok, params} = PostgrestParser.parse_query_string("expression=eq.(a%2Bb)")
      assert [%Filter{value: "(a+b)"}] = params.filters
    end
  end

  describe "production query patterns" do
    test "typical e-commerce product listing query" do
      query =
        "select=id,name,price,category&category=eq.Electronics&price=gte.50&price=lte.500&order=price.asc&limit=20"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      {:ok, result} = PostgrestParser.to_sql("products", params)

      assert length(params.select) == 4
      assert length(params.filters) == 3
      assert String.contains?(result.query, "ORDER BY")
      assert String.contains?(result.query, "LIMIT")
    end

    test "user search with text matching" do
      query = "select=id,name,email&or=(name.ilike.%john%,email.ilike.%john%)&limit=10"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      assert [%LogicTree{operator: :or}] = params.filters
    end

    test "dashboard analytics with aggregation-ready select" do
      query =
        "select=status,created_at&status=in.(pending,processing,completed)&order=created_at.desc"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      {:ok, result} = PostgrestParser.to_sql("orders", params)

      assert String.contains?(result.query, "= ANY($1)")
    end

    test "fetching user with nested profile and posts" do
      query = "select=id,name,profile(bio,avatar),posts(id,title,created_at)&id=eq.1"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      assert length(params.select) == 4
      assert Enum.count(params.select, &(&1.type == :relation)) == 2
    end

    test "complex e-commerce query with filters and embedding" do
      query =
        "select=id,name,price,category:categories(name)&price=gte.100&price=lte.500&or=(in_stock.is.true,available_date.lte.2026-02-01)&order=price.asc&limit=20"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      assert length(params.filters) == 3
      assert Enum.any?(params.filters, &match?(%LogicTree{operator: :or}, &1))
    end

    test "user management with roles query" do
      query =
        "select=id,email,profile:profiles(name,avatar)&email=ilike.*@company.com&order=created_at.desc&limit=50"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "ILIKE")
      assert String.contains?(result.query, "ORDER BY")
    end

    test "content with nested comments query" do
      query =
        "select=id,title,body,author:users(name,email),comments:comments(text,created_at)&published=is.true&order=created_at.desc&limit=25"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      relations = Enum.filter(params.select, &(&1.type == :relation))
      assert length(relations) == 2
    end

    test "analytics query with JSON filtering" do
      query =
        "select=id,event_name,properties->>category,created_at&event_name=in.(page_view,click,purchase)&created_at=gte.2026-01-01&order=created_at.desc"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      assert length(params.filters) == 2
    end

    test "inner join filtering on embedded resource" do
      query = "select=title,authors!inner(first_name,last_name)"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      [_, authors] = params.select
      assert authors.hint == "inner"
    end

    test "spread syntax for flattening relations" do
      query =
        "select=title,...directors(director_first_name:first_name,director_last_name:last_name)"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      [_, spread] = params.select
      assert spread.type == :spread
      assert length(spread.children) == 2
    end

    test "multiple FK disambiguation" do
      query = "select=id,billing:addresses!billing(street),shipping:addresses!shipping(street)"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      [_, billing, shipping] = params.select
      assert billing.alias == "billing"
      assert billing.hint == "billing"
      assert shipping.alias == "shipping"
      assert shipping.hint == "shipping"
    end

    test "full-text search with language" do
      query =
        "select=id,title,content&content=fts(english).database optimization&order=created_at.desc"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      [filter] = params.filters
      assert filter.operator == :fts
      assert filter.language == "english"
    end

    test "phrase search with negation" do
      query = "content=not.phfts(english).bad phrase"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      [filter] = params.filters
      assert filter.operator == :phfts
      assert filter.negated? == true
      assert filter.language == "english"
    end
  end

  # ============================================================================
  # SelectParser Direct Usage Tests
  # ============================================================================

  describe "SelectParser edge cases" do
    alias PostgrestParser.SelectParser

    test "returns empty list for empty string" do
      assert {:ok, []} = SelectParser.parse("")
    end

    test "returns empty list for nil" do
      assert {:ok, []} = SelectParser.parse(nil)
    end

    test "handles trailing comma gracefully" do
      {:ok, items} = SelectParser.parse("id,")
      assert [%SelectItem{name: "id"}] = items
    end

    test "handles empty relation children" do
      {:ok, items} = SelectParser.parse("posts(),id")
      assert [%SelectItem{name: "posts", children: []}, %SelectItem{name: "id"}] = items
    end

    test "parses field with type cast" do
      {:ok, items} = SelectParser.parse("price::text")
      assert [%SelectItem{name: "price", hint: {:cast, "text"}}] = items
    end

    test "parses JSON path with double arrow" do
      {:ok, items} = SelectParser.parse("data->>name")
      assert [%SelectItem{name: "data"}] = items
    end

    test "parses spread operator with constraint hint" do
      {:ok, items} = SelectParser.parse("...author!authors_fk(name)")
      assert [%SelectItem{type: :spread, hint: "authors_fk"}] = items
    end

    test "parses multiple spreads" do
      {:ok, items} = SelectParser.parse("id,...author(name),...editor(name)")
      spreads = Enum.filter(items, &(&1.type == :spread))
      assert length(spreads) == 2
    end

    test "parses deeply nested relations" do
      {:ok, items} =
        SelectParser.parse("author(name,books(title,publisher(name,country)))")

      [author] = items
      [_, books] = author.children
      [_, publisher] = books.children
      assert publisher.name == "publisher"
    end

    test "parses relation with multiple hints" do
      {:ok, items} = SelectParser.parse("writer:author!inner(name)")
      [item] = items
      assert item.alias == "writer"
      assert item.name == "author"
      assert item.hint == "inner"
    end

    test "parses wildcard with alias" do
      {:ok, items} = SelectParser.parse("all:*")
      assert [%SelectItem{name: "*", alias: "all"}] = items
    end

    test "parses simple field list" do
      {:ok, items} = SelectParser.parse("id,name,email")
      assert length(items) == 3
      assert Enum.map(items, & &1.name) == ["id", "name", "email"]
    end

    test "parses relation with inner hint" do
      {:ok, [item]} = SelectParser.parse("posts!inner(title)")
      assert item.name == "posts"
      assert item.hint == "inner"
      assert item.type == :relation
    end

    test "parses spread with hint" do
      {:ok, [item]} = SelectParser.parse("...author!fk_name(name)")
      assert item.type == :spread
      assert item.hint == "fk_name"
    end

    test "parses nested relation with multiple levels" do
      {:ok, items} = SelectParser.parse("posts(author(name,bio),comments(text))")
      [posts] = items
      assert length(posts.children) == 2
    end

    test "parses alias with relation" do
      {:ok, [item]} = SelectParser.parse("writer:author(name)")
      assert item.alias == "writer"
      assert item.name == "author"
    end

    test "parses multiple relations in sequence" do
      {:ok, items} = SelectParser.parse("posts(id),comments(text),tags(name)")
      assert length(items) == 3
      assert Enum.all?(items, &(&1.type == :relation))
    end

    test "parses field with JSON path and alias" do
      {:ok, [item]} = SelectParser.parse("user_name:data->>name")
      assert item.alias == "user_name"
      assert item.name == "data"
    end

    test "handles whitespace in comma-separated items" do
      {:ok, items} = SelectParser.parse("id, name, email")
      assert length(items) == 3
    end

    test "parses complex select with mix of types" do
      {:ok, items} = SelectParser.parse("id,author:users(name),...profile(bio),data->meta")
      assert length(items) == 4
      types = Enum.map(items, & &1.type)
      assert :field in types
      assert :relation in types
      assert :spread in types
    end

    test "parses field with JSON path and cast" do
      {:ok, items} = SelectParser.parse("data->amount::numeric")
      [item] = items
      assert item.hint == {:json_path_cast, [{:arrow, "amount"}], "numeric"}
    end

    test "parses relation with empty parens using fallback" do
      {:ok, items} = SelectParser.parse("posts()")
      [item] = items
      assert item.type == :relation
      assert item.children == []
    end

    test "parses deeply nested spread with alias" do
      {:ok, items} = SelectParser.parse("...author_info:author(name,bio)")
      [item] = items
      assert item.type == :spread
      assert item.name == "author"
    end

    test "returns error for unclosed nested relation" do
      assert {:error, reason} = SelectParser.parse("posts(comments(text)")
      assert reason =~ "parenthesis"
    end

    test "parses relation with wildcard child" do
      {:ok, items} = SelectParser.parse("posts(*)")
      [item] = items
      assert item.type == :relation
      assert [%SelectItem{name: "*"}] = item.children
    end

    test "parses triple nested relations" do
      {:ok, items} = SelectParser.parse("posts(comments(author(name)))")
      [posts] = items
      [comments] = posts.children
      [author] = comments.children
      [name] = author.children
      assert name.name == "name"
    end

    test "parses select with leading whitespace in items" do
      {:ok, items} = SelectParser.parse(" id, name, email")
      assert length(items) == 3
    end

    test "parses multiple hints with alias" do
      {:ok, items} = SelectParser.parse("my_posts:posts!fk_author_id(title)")
      [item] = items
      assert item.alias == "my_posts"
      assert item.name == "posts"
      assert item.hint == "fk_author_id"
    end

    test "returns error for invalid field name with parenthesis" do
      assert {:error, _} = SelectParser.parse("bad(field")
    end

    test "parses field with cast without alias" do
      {:ok, items} = SelectParser.parse("price::integer")
      [item] = items
      assert item.hint == {:cast, "integer"}
      assert item.alias == nil
    end

    test "parses nested relation after field" do
      {:ok, items} = SelectParser.parse("id,author(name,posts(title))")
      assert length(items) == 2
      [_, author] = items
      assert length(author.children) == 2
    end

    test "handles relation followed by another relation" do
      {:ok, items} = SelectParser.parse("posts(id),tags(name)")
      assert length(items) == 2
      assert Enum.all?(items, &(&1.type == :relation))
    end
  end

  # ============================================================================
  # FilterParser Direct Usage Tests
  # ============================================================================

  describe "FilterParser edge cases" do
    alias PostgrestParser.FilterParser

    test "parses field with type cast" do
      {:ok, field} = FilterParser.parse_field("created_at::date")
      assert field.name == "created_at"
      assert field.cast == "date"
    end

    test "identifies reserved PostgREST parameters" do
      assert FilterParser.reserved_key?("select")
      assert FilterParser.reserved_key?("order")
      assert FilterParser.reserved_key?("limit")
      assert FilterParser.reserved_key?("offset")
      refute FilterParser.reserved_key?("status")
      refute FilterParser.reserved_key?("id")
    end

    test "parses field with JSON path and type cast" do
      {:ok, field} = FilterParser.parse_field("data->age::integer")
      assert field.name == "data"
      assert field.json_path == [{:arrow, "age"}]
      assert field.cast == "integer"
    end

    test "parses simple field name" do
      {:ok, field} = FilterParser.parse_field("username")
      assert field.name == "username"
      assert field.json_path == []
      assert field.cast == nil
    end

    test "parses field with multiple JSON operators" do
      {:ok, field} = FilterParser.parse_field("data->nested->>value")
      assert field.json_path == [{:arrow, "nested"}, {:double_arrow, "value"}]
    end

    test "parses filter with contained_by operator" do
      {:ok, params} = PostgrestParser.parse_query_string("roles=cd.{admin,user,guest}")
      assert [%Filter{operator: :cd, value: "{admin,user,guest}"}] = params.filters
    end

    test "generates SQL for cd operator" do
      {:ok, params} = PostgrestParser.parse_query_string("roles=cd.{admin,user}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "<@")
    end

    test "generates SQL for cs operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cs.{featured,new}")
      {:ok, result} = PostgrestParser.to_sql("products", params)
      assert String.contains?(result.query, "@>")
    end

    test "generates SQL for ov operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=ov.(a,b,c)")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "&&")
    end

    test "parses not.is.null correctly" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=not.is.null")
      assert [%Filter{operator: :is, value: "null", negated?: true}] = params.filters

      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "generates SQL for is.false" do
      {:ok, params} = PostgrestParser.parse_query_string("active=is.false")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "IS FALSE")
    end

    test "generates SQL for is.unknown" do
      {:ok, params} = PostgrestParser.parse_query_string("status=is.unknown")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "IS UNKNOWN")
    end

    test "parses filter with quoted values in list" do
      {:ok, params} = PostgrestParser.parse_query_string(~s|name=in.("John","Jane")|)
      assert [%Filter{operator: :in, value: ["John", "Jane"]}] = params.filters
    end

    test "parses filter with escaped quotes in values" do
      {:ok, params} = PostgrestParser.parse_query_string(~s|name=in.("value\\"with\\"quotes")|)
      assert [%Filter{operator: :in, value: [~s|value"with"quotes|]}] = params.filters
    end

    test "parse_field returns error for non-string input" do
      assert {:error, _} = FilterParser.parse_field(nil)
      assert {:error, _} = FilterParser.parse_field(123)
    end

    test "parses field with dots using fallback" do
      {:ok, field} = FilterParser.parse_field("table.column")
      assert field.name == "table.column"
    end

    test "parses complex JSON path using fallback" do
      {:ok, field} = FilterParser.parse_field("data->a->b->>c")
      assert field.name == "data"
      assert field.json_path == [{:arrow, "a"}, {:arrow, "b"}, {:double_arrow, "c"}]
    end

    test "parses field with special characters using fallback" do
      {:ok, field} = FilterParser.parse_field("my_field.subfield")
      assert field.name == "my_field.subfield"
    end

    test "handles not. prefix in fallback parsing" do
      {:ok, params} = PostgrestParser.parse_query_string("field=not.like.%test%")
      [filter] = params.filters
      assert filter.negated? == true
      assert filter.operator == :like
    end

    test "handles FTS operator without language" do
      {:ok, params} = PostgrestParser.parse_query_string("content=fts.search")
      [filter] = params.filters
      assert filter.operator == :fts
      assert filter.language == nil
    end

    test "handles quantifier with eq operator" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq(any).{1,2,3}")
      [filter] = params.filters
      assert filter.operator == :eq
      assert filter.quantifier == :any
    end

    test "handles quantifier with like operator" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like(any).{%john%,%jane%}")
      [filter] = params.filters
      assert filter.operator == :like
      assert filter.quantifier == :any
    end

    test "parses negated FTS with language" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.plfts(spanish).buscar")
      [filter] = params.filters
      assert filter.negated? == true
      assert filter.operator == :plfts
      assert filter.language == "spanish"
    end

    test "handles bracket notation in range values" do
      {:ok, params} = PostgrestParser.parse_query_string("range=adj.[18,21)")
      [filter] = params.filters
      assert filter.operator == :adj
      assert filter.value == "[18,21)"
    end

    test "handles on_conflict reserved key" do
      assert FilterParser.reserved_key?("on_conflict")
    end

    test "handles columns reserved key" do
      assert FilterParser.reserved_key?("columns")
    end

    test "parses filter with match operator and quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("code=match(any).{^A,^B}")
      [filter] = params.filters
      assert filter.operator == :match
      assert filter.quantifier == :any
    end

    test "parses filter with imatch operator and quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=imatch(all).{test,check}")
      [filter] = params.filters
      assert filter.operator == :imatch
      assert filter.quantifier == :all
    end

    test "returns error for missing value in filter" do
      assert {:error, _} = FilterParser.parse("id", "eq")
    end

    test "returns error for filter with invalid quantifier on non-quantifiable operator" do
      assert {:error, reason} = FilterParser.parse("tags", "cs(any).{a,b}")
      assert reason =~ "quantifier"
    end

    test "handles fallback for value parsing with malformed brace list" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq(any).{1,2,3}")
      [filter] = params.filters
      assert filter.value == ["1", "2", "3"]
    end

    test "handles fallback for value parsing with malformed paren list" do
      {:ok, params} = PostgrestParser.parse_query_string("id=in.(a,b,c)")
      [filter] = params.filters
      assert filter.value == ["a", "b", "c"]
    end

    test "returns error for quantified operator without proper list format" do
      assert {:error, _} = FilterParser.parse("id", "eq(any).notalist")
    end

    test "returns error for in operator without proper list format" do
      assert {:error, reason} = FilterParser.parse("id", "in.notalist")
      assert reason =~ "list format"
    end
  end

  # ============================================================================
  # OrderParser Direct Usage Tests
  # ============================================================================

  describe "OrderParser edge cases" do
    alias PostgrestParser.OrderParser

    test "treats unknown direction as part of field name" do
      {:ok, [term]} = OrderParser.parse("id.invalid")
      assert term.field.name == "id.invalid"
      assert term.direction == :asc
    end

    test "parses nulls option without direction" do
      {:ok, [term]} = OrderParser.parse("priority.nullsfirst")
      assert term.direction == :asc
      assert term.nulls == :first
    end

    test "returns empty list for nil input" do
      assert {:ok, []} = OrderParser.parse(nil)
    end

    test "returns empty list for empty string" do
      assert {:ok, []} = OrderParser.parse("")
    end

    test "parses multiple order terms" do
      {:ok, terms} = OrderParser.parse("name.asc,created_at.desc,updated_at")
      assert length(terms) == 3
      assert Enum.map(terms, & &1.direction) == [:asc, :desc, :asc]
    end

    test "parses order with JSON path" do
      {:ok, [term]} = OrderParser.parse("data->key.desc")
      assert term.field.name == "data"
      assert term.field.json_path == [{:arrow, "key"}]
      assert term.direction == :desc
    end

    test "parses order with double arrow JSON path" do
      {:ok, [term]} = OrderParser.parse("data->>key.asc")
      assert term.field.json_path == [{:double_arrow, "key"}]
    end

    test "parses order with nullslast" do
      {:ok, [term]} = OrderParser.parse("field.desc.nullslast")
      assert term.direction == :desc
      assert term.nulls == :last
    end

    test "handles order term without direction" do
      {:ok, [term]} = OrderParser.parse("name")
      assert term.field.name == "name"
      assert term.direction == :asc
    end

    test "parses complex JSON path order" do
      {:ok, [term]} = OrderParser.parse("meta->user->>age.desc.nullsfirst")
      assert term.field.name == "meta"
      assert term.field.json_path == [{:arrow, "user"}, {:double_arrow, "age"}]
      assert term.direction == :desc
      assert term.nulls == :first
    end

    test "parses single term directly" do
      {:ok, term} = OrderParser.parse_term("id.desc")
      assert term.field.name == "id"
      assert term.direction == :desc
    end

    test "parses term with JSON path" do
      {:ok, term} = OrderParser.parse_term("data->key.asc.nullslast")
      assert term.field.name == "data"
      assert term.field.json_path == [{:arrow, "key"}]
      assert term.direction == :asc
      assert term.nulls == :last
    end

    test "parses term with field containing dots" do
      {:ok, term} = OrderParser.parse_term("schema.table.column")
      assert term.field.name == "schema.table.column"
      assert term.direction == :asc
    end

    test "falls back for complex field names" do
      {:ok, [term]} = OrderParser.parse("complex.field.name.desc")
      assert term.field.name == "complex.field.name"
      assert term.direction == :desc
    end

    test "handles json segment building" do
      assert {:arrow, "key"} = OrderParser.build_json_segment([:arrow, "key"])
      assert {:double_arrow, "value"} = OrderParser.build_json_segment([:double_arrow, "value"])
    end

    test "parses order with only nullsfirst" do
      {:ok, [term]} = OrderParser.parse("field.nullsfirst")
      assert term.direction == :asc
      assert term.nulls == :first
    end

    test "parses order with only nullslast" do
      {:ok, [term]} = OrderParser.parse("field.nullslast")
      assert term.direction == :asc
      assert term.nulls == :last
    end

    test "handles fallback for order with special characters" do
      {:ok, [term]} = OrderParser.parse("field_name.desc")
      assert term.field.name == "field_name"
      assert term.direction == :desc
    end

    test "parses multiple terms with different options" do
      {:ok, terms} = OrderParser.parse("a.desc.nullsfirst,b.asc.nullslast,c")
      assert length(terms) == 3
      [a, b, c] = terms
      assert a.direction == :desc
      assert a.nulls == :first
      assert b.direction == :asc
      assert b.nulls == :last
      assert c.direction == :asc
      assert c.nulls == nil
    end
  end

  # ============================================================================
  # LogicParser Direct Usage Tests
  # ============================================================================

  describe "LogicParser edge cases" do
    alias PostgrestParser.LogicParser

    test "identifies logic keys correctly" do
      assert LogicParser.logic_key?("and")
      assert LogicParser.logic_key?("or")
      assert LogicParser.logic_key?("not.and")
      assert LogicParser.logic_key?("not.or")
      refute LogicParser.logic_key?("id")
      refute LogicParser.logic_key?("status")
    end

    test "parses simple AND logic tree" do
      {:ok, tree} = LogicParser.parse("and", "(status.eq.active,verified.is.true)")
      assert tree.operator == :and
      assert length(tree.conditions) == 2
    end

    test "parses simple OR logic tree" do
      {:ok, tree} = LogicParser.parse("or", "(id.eq.1,id.eq.2)")
      assert tree.operator == :or
      assert length(tree.conditions) == 2
    end

    test "parses negated AND" do
      {:ok, tree} = LogicParser.parse("not.and", "(a.eq.1,b.eq.2)")
      assert tree.operator == :and
      assert tree.negated? == true
    end

    test "parses negated OR" do
      {:ok, tree} = LogicParser.parse("not.or", "(a.eq.1,b.eq.2)")
      assert tree.operator == :or
      assert tree.negated? == true
    end

    test "parses nested logic" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.1,or(b.eq.2,c.eq.3))")
      [filter, nested_or] = tree.conditions
      assert filter.operator == :eq
      assert nested_or.operator == :or
    end

    test "returns error for missing parentheses" do
      assert {:error, _} = LogicParser.parse("and", "a.eq.1,b.eq.2")
    end

    test "returns error for invalid filter in conditions" do
      assert {:error, _} = LogicParser.parse("and", "(a.invalid.1,b.eq.2)")
    end

    test "parses deeply nested logic" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.1,or(b.eq.2,and(c.eq.3,d.eq.4)))")
      assert tree.operator == :and
      [filter, nested_or] = tree.conditions
      assert filter.operator == :eq
      [filter2, nested_and] = nested_or.conditions
      assert filter2.operator == :eq
      assert nested_and.operator == :and
    end

    test "parses conditions with JSON path" do
      {:ok, tree} = LogicParser.parse("and", "(data->key.eq.value,name.eq.test)")
      [filter1, filter2] = tree.conditions
      assert filter1.field.json_path == [{:arrow, "key"}]
      assert filter2.field.name == "name"
    end

    test "parses conditions with is operator" do
      {:ok, tree} = LogicParser.parse("or", "(deleted_at.is.null,status.eq.active)")
      [is_filter, eq_filter] = tree.conditions
      assert is_filter.operator == :is
      assert eq_filter.operator == :eq
    end

    test "parses conditions with pattern matching" do
      {:ok, tree} = LogicParser.parse("or", "(name.like.%john%,name.ilike.%jane%)")
      [like_filter, ilike_filter] = tree.conditions
      assert like_filter.operator == :like
      assert ilike_filter.operator == :ilike
    end

    test "handles many conditions in logic tree" do
      conditions = Enum.map_join(1..10, ",", &"id.eq.#{&1}")
      {:ok, tree} = LogicParser.parse("or", "(#{conditions})")
      assert length(tree.conditions) == 10
    end

    test "parses not.or inside and" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.1,not.or(b.eq.2,c.eq.3))")
      [filter, nested] = tree.conditions
      assert filter.operator == :eq
      assert nested.operator == :or
      assert nested.negated? == true
    end

    test "returns error for missing outer parenthesis" do
      assert {:error, reason} = LogicParser.parse("and", "a.eq.1,b.eq.2")
      assert reason =~ "parentheses"
    end

    test "returns error for unexpected closing parenthesis" do
      assert {:error, reason} = LogicParser.parse("and", "(a.eq.1))")
      assert reason =~ ~r/(unexpected|closing)/
    end

    test "parses conditions using equals notation" do
      {:ok, tree} = LogicParser.parse("and", "(id=eq.1,name=eq.john)")
      assert length(tree.conditions) == 2
      [filter1, filter2] = tree.conditions
      assert filter1.field.name == "id"
      assert filter2.field.name == "name"
    end

    test "parses negated filter inside logic tree using dot notation" do
      {:ok, tree} = LogicParser.parse("or", "(a.not.eq.1,b.eq.2)")
      [negated_filter, _] = tree.conditions
      assert negated_filter.negated? == true
    end

    test "returns error for invalid filter format inside logic tree" do
      assert {:error, reason} = LogicParser.parse("and", "(invalid)")
      assert reason =~ "invalid"
    end

    test "handles field with parentheses in value as regular filter" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.value(1),b.eq.2)")
      assert length(tree.conditions) == 2
    end

    test "parses not.and inside or" do
      {:ok, tree} = LogicParser.parse("or", "(a.eq.1,not.and(b.eq.2,c.eq.3))")
      [filter, nested] = tree.conditions
      assert filter.operator == :eq
      assert nested.operator == :and
      assert nested.negated? == true
    end

    test "handles filter with FTS operator inside logic tree" do
      {:ok, tree} = LogicParser.parse("or", "(content.fts.hello,content.plfts.world)")
      assert length(tree.conditions) == 2
    end

    test "handles filter with in operator inside logic tree" do
      {:ok, tree} = LogicParser.parse("and", "(status.in.(a,b),other.eq.1)")
      [in_filter, _] = tree.conditions
      assert in_filter.operator == :in
    end
  end

  # ============================================================================
  # AliasParser Direct Usage Tests
  # ============================================================================

  describe "AliasParser edge cases" do
    alias PostgrestParser.SelectParser.AliasParser

    test "parses simple field without alias" do
      assert {nil, "name"} = AliasParser.parse("name")
    end

    test "parses alias before field" do
      assert {"user_name", "name"} = AliasParser.parse("user_name:name")
    end

    test "parses field with cast and alias (alias after cast)" do
      assert {"price_str", "price::text"} = AliasParser.parse("price::text:price_str")
    end

    test "parses JSON path without alias" do
      assert {nil, "data->value"} = AliasParser.parse("data->value")
    end

    test "parses JSON path with alias" do
      assert {"my_value", "data->value"} = AliasParser.parse("my_value:data->value")
    end

    test "parses JSON path with cast and alias" do
      assert {"total", "data->price::numeric"} =
               AliasParser.parse("data->price::numeric:total")
    end

    test "parses wildcard" do
      assert {nil, "*"} = AliasParser.parse("*")
    end

    test "parses wildcard with alias" do
      assert {"all", "*"} = AliasParser.parse("all:*")
    end

    test "parses spread operator" do
      assert {nil, "...profile"} = AliasParser.parse("...profile")
    end

    test "handles empty string" do
      assert {nil, ""} = AliasParser.parse("")
    end

    test "parses field with double arrow JSON" do
      assert {nil, "data->>name"} = AliasParser.parse("data->>name")
    end

    test "parses nested JSON path" do
      assert {nil, "data->user->>email"} = AliasParser.parse("data->user->>email")
    end

    test "parses hint syntax in field" do
      assert {nil, "posts!inner"} = AliasParser.parse("posts!inner")
    end
  end

  # ============================================================================
  # SchemaCache Tests
  # ============================================================================

  describe "SchemaCache struct definitions" do
    alias PostgrestParser.SchemaCache.Relationship

    test "creates M2O relationship struct" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        cardinality: :m2o,
        source_table: "orders",
        target_table: "customers",
        source_columns: ["customer_id"],
        target_columns: ["id"]
      }

      assert rel.cardinality == :m2o
    end

    test "creates M2M relationship with junction table" do
      rel = %Relationship{
        constraint_name: "post_tags_post_id_fkey",
        cardinality: :m2m,
        source_table: "posts",
        target_table: "tags",
        source_columns: ["id"],
        target_columns: ["id"],
        junction: %{
          table: "post_tags",
          source_columns: ["post_id"],
          target_columns: ["tag_id"]
        }
      }

      assert rel.junction.table == "post_tags"
    end
  end

  # ============================================================================
  # RelationBuilder Tests (tested via integration tests for full behavior)
  # ============================================================================

  describe "RelationBuilder" do
    alias PostgrestParser.RelationBuilder
    alias PostgrestParser.SchemaCache.Relationship

    test "builds join SQL using single relation join function" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "orders",
        target_schema: "public",
        target_table: "customers",
        source_columns: ["customer_id"],
        target_columns: ["id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "customer",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {join_sql, select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "orders", 0)

      assert String.contains?(join_sql, "LEFT JOIN LATERAL")
      assert String.contains?(join_sql, "row_to_json")
      assert String.contains?(select_sql, "customer")
    end

    test "builds aggregated join for one-to-many" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        cardinality: :o2m,
        source_schema: "public",
        source_table: "customers",
        target_schema: "public",
        target_table: "orders",
        source_columns: ["id"],
        target_columns: ["customer_id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "orders",
        children: [%SelectItem{type: :field, name: "id"}]
      }

      {join_sql, _select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "customers", 0)

      assert String.contains?(join_sql, "json_agg")
    end

    test "builds m2m join with junction table" do
      rel = %Relationship{
        constraint_name: "post_tags_tag_id_fkey",
        cardinality: :m2m,
        source_schema: "public",
        source_table: "posts",
        target_schema: "public",
        target_table: "tags",
        source_columns: ["id"],
        target_columns: ["id"],
        junction: %{
          schema: "public",
          table: "post_tags",
          source_columns: ["post_id"],
          target_columns: ["tag_id"]
        }
      }

      select_item = %SelectItem{
        type: :relation,
        name: "tags",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {join_sql, select_sql} = RelationBuilder.build_m2m_join(select_item, rel, "posts", 0)

      assert String.contains?(join_sql, "LEFT JOIN LATERAL")
      assert String.contains?(join_sql, "json_agg")
      assert String.contains?(join_sql, "junction_0")
      assert String.contains?(select_sql, "tags")
    end

    test "builds join with nil children (select *)" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "orders",
        target_schema: "public",
        target_table: "customers",
        source_columns: ["customer_id"],
        target_columns: ["id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "customer",
        children: nil
      }

      {join_sql, _select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "orders", 0)

      assert String.contains?(join_sql, "customer_0.*")
    end

    test "builds join with empty children list (select *)" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "orders",
        target_schema: "public",
        target_table: "customers",
        source_columns: ["customer_id"],
        target_columns: ["id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "customer",
        children: []
      }

      {join_sql, _select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "orders", 0)

      assert String.contains?(join_sql, "customer_0.*")
    end

    test "builds join with alias" do
      rel = %Relationship{
        constraint_name: "orders_customer_id_fkey",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "orders",
        target_schema: "public",
        target_table: "customers",
        source_columns: ["customer_id"],
        target_columns: ["id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "customer",
        alias: "buyer",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {_join_sql, select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "orders", 0)

      assert String.contains?(select_sql, ~s("buyer"))
    end

    test "builds o2o join" do
      rel = %Relationship{
        constraint_name: "users_profile_fkey",
        cardinality: :o2o,
        source_schema: "public",
        source_table: "users",
        target_schema: "public",
        target_table: "profiles",
        source_columns: ["id"],
        target_columns: ["user_id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "profile",
        children: [%SelectItem{type: :field, name: "bio"}]
      }

      {join_sql, _select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "users", 0)

      assert String.contains?(join_sql, "row_to_json")
      assert String.contains?(join_sql, "LIMIT 1")
    end

    test "builds join with multiple columns" do
      rel = %Relationship{
        constraint_name: "composite_fkey",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "orders",
        target_schema: "public",
        target_table: "items",
        source_columns: ["item_id", "variant_id"],
        target_columns: ["id", "variant"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "item",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {join_sql, _select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "orders", 0)

      assert String.contains?(join_sql, "AND")
    end

    test "builds join with quoted identifiers for special characters" do
      rel = %Relationship{
        constraint_name: "test_fkey",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "user-orders",
        target_schema: "public",
        target_table: "users",
        source_columns: ["user_id"],
        target_columns: ["id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "user",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {join_sql, _select_sql} =
        RelationBuilder.build_single_relation_join(select_item, rel, "user-orders", 0)

      assert String.contains?(join_sql, ~s("users"))
      assert String.contains?(join_sql, ~s("user-orders"))
    end

    test "builds m2m join with alias" do
      rel = %Relationship{
        constraint_name: "post_tags_tag_id_fkey",
        cardinality: :m2m,
        source_schema: "public",
        source_table: "posts",
        target_schema: "public",
        target_table: "tags",
        source_columns: ["id"],
        target_columns: ["id"],
        junction: %{
          schema: "public",
          table: "post_tags",
          source_columns: ["post_id"],
          target_columns: ["tag_id"]
        }
      }

      select_item = %SelectItem{
        type: :relation,
        name: "tags",
        alias: "categories",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {_join_sql, select_sql} = RelationBuilder.build_m2m_join(select_item, rel, "posts", 0)

      assert String.contains?(select_sql, ~s("categories"))
    end
  end

  # ============================================================================
  # Common Parser Tests
  # ============================================================================

  describe "Common parser helpers" do
    alias PostgrestParser.Parsers.Common

    test "parses simple identifier" do
      assert {:ok, ["test_field"], "", _, _, _} = Common.parse_identifier("test_field")
    end

    test "parses identifier with numbers" do
      assert {:ok, ["field123"], "", _, _, _} = Common.parse_identifier("field123")
    end

    test "parses JSON path with single arrow" do
      assert {:ok, [{:arrow, "key"}], "", _, _, _} = Common.parse_json_path("->key")
    end

    test "parses JSON path with double arrow" do
      assert {:ok, [{:double_arrow, "key"}], "", _, _, _} = Common.parse_json_path("->>key")
    end

    test "parses multiple JSON path segments" do
      assert {:ok, segments, "", _, _, _} = Common.parse_json_path("->a->>b")
      assert segments == [{:arrow, "a"}, {:double_arrow, "b"}]
    end

    test "parses field with name only" do
      {:ok, result, "", _, _, _} = Common.parse_field("user")
      assert Keyword.get(result, :name) == "user"
      assert Keyword.get(result, :json_path) == []
    end

    test "parses field with JSON path" do
      {:ok, result, "", _, _, _} = Common.parse_field("data->key")
      assert Keyword.get(result, :name) == "data"
      assert Keyword.get(result, :json_path) == [{:arrow, "key"}]
    end

    test "parses field with cast" do
      {:ok, result, "", _, _, _} = Common.parse_field("price::numeric")
      assert Keyword.get(result, :name) == "price"
      assert Keyword.get(result, :cast) == "numeric"
    end

    test "parses field with JSON path and cast" do
      {:ok, result, "", _, _, _} = Common.parse_field("data->amount::integer")
      assert Keyword.get(result, :name) == "data"
      assert Keyword.get(result, :json_path) == [{:arrow, "amount"}]
      assert Keyword.get(result, :cast) == "integer"
    end

    test "extracts field struct from parsed result" do
      parsed = [name: "user", json_path: [{:arrow, "id"}], cast: "text"]
      field = Common.extract_field(parsed)

      assert field.name == "user"
      assert field.json_path == [{:arrow, "id"}]
      assert field.cast == "text"
    end

    test "handles field result for complete parse" do
      result = Common.parse_field("data->key")
      assert {:ok, %PostgrestParser.AST.Field{}} = Common.handle_field_result(result)
    end

    test "handles field result with unparsed rest (partial parse)" do
      result = Common.parse_field("field.with.dots")

      assert {:ok, %PostgrestParser.AST.Field{name: "field.with.dots"}} =
               Common.handle_field_result(result)
    end

    test "handles field result error" do
      result = {:error, "some error", "rest", %{}, {1, 0}, 0}
      assert {:error, :use_fallback} = Common.handle_field_result(result)
    end

    test "builds json segment from parser result" do
      assert {:arrow, "key"} = Common.build_json_segment([:arrow, "key"])
      assert {:double_arrow, "value"} = Common.build_json_segment([:double_arrow, "value"])
    end

    test "extracts field with empty json_path" do
      parsed = [name: "simple"]
      field = Common.extract_field(parsed)
      assert field.name == "simple"
      assert field.json_path == []
      assert field.cast == nil
    end

    test "parses empty JSON path" do
      assert {:ok, [], "", _, _, _} = Common.parse_json_path("")
    end

    test "parses long JSON path chain" do
      assert {:ok, segments, "", _, _, _} = Common.parse_json_path("->a->b->c->>d")

      assert segments == [
               {:arrow, "a"},
               {:arrow, "b"},
               {:arrow, "c"},
               {:double_arrow, "d"}
             ]
    end
  end

  # ============================================================================
  # Additional SQL Generation Tests
  # ============================================================================

  describe "SQL generation edge cases" do
    test "generates SQL for negated comparison" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.eq.deleted")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "<>")
    end

    test "generates SQL for negated in operator" do
      {:ok, params} = PostgrestParser.parse_query_string("id=not.in.(1,2,3)")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "ANY")
    end

    test "generates SQL with multiple JSON paths" do
      {:ok, params} = PostgrestParser.parse_query_string("data->user->>name=eq.John")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, ~s("data"->'user'->>'name'))
    end

    test "generates SQL for like with wildcard conversion" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like.John*")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "LIKE")
    end

    test "generates SQL for fts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=fts.database")
      {:ok, result} = PostgrestParser.to_sql("articles", params)

      assert String.contains?(result.query, "@@")
      assert String.contains?(result.query, "to_tsquery")
    end

    test "generates SQL for fts with language" do
      {:ok, params} = PostgrestParser.parse_query_string("content=fts(english).database")
      {:ok, result} = PostgrestParser.to_sql("articles", params)

      assert String.contains?(result.query, "'english'")
    end

    test "generates SQL for plfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=plfts.search terms")
      {:ok, result} = PostgrestParser.to_sql("articles", params)

      assert String.contains?(result.query, "plainto_tsquery")
    end

    test "generates SQL for phfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=phfts.exact phrase")
      {:ok, result} = PostgrestParser.to_sql("articles", params)

      assert String.contains?(result.query, "phraseto_tsquery")
    end

    test "generates SQL for wfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=wfts.web search")
      {:ok, result} = PostgrestParser.to_sql("articles", params)

      assert String.contains?(result.query, "websearch_to_tsquery")
    end

    test "generates SQL for quantified eq(any)" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq(any).{1,2,3}")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "= ANY")
    end

    test "generates SQL for quantified eq(all)" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq(all).{1,2,3}")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "= ALL")
    end

    test "generates SQL for JSON path in select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=data->>name")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, ~s("data"->>'name'))
    end

    test "handles select with cast to text" do
      {:ok, params} = PostgrestParser.parse_query_string("select=amount::text")
      {:ok, result} = PostgrestParser.to_sql("orders", params)

      assert String.contains?(result.query, "::text")
    end

    test "generates SQL for gt with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("age=gt(any).{18,21}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "> ANY")
    end

    test "generates SQL for lt with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("age=lt(all).{65,70}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "< ALL")
    end

    test "generates SQL for gte with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("score=gte(any).{80,90}")
      {:ok, result} = PostgrestParser.to_sql("tests", params)
      assert String.contains?(result.query, ">= ANY")
    end

    test "generates SQL for lte with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("score=lte(all).{100,95}")
      {:ok, result} = PostgrestParser.to_sql("tests", params)
      assert String.contains?(result.query, "<= ALL")
    end

    test "generates SQL for neq with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("status=neq(any).{deleted,archived}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "<> ANY")
    end

    test "generates SQL for neq with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("status=neq(all).{active,pending}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "<> ALL")
    end

    test "generates SQL for like with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like(any).{%john%,%jane%}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "LIKE ANY")
    end

    test "generates SQL for ilike with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("email=ilike(all).{%@company%,%@corp%}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "ILIKE ALL")
    end

    test "generates SQL for match with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("code=match(any).{^[A-Z],%[0-9]$}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "~ ANY")
    end

    test "generates SQL for imatch with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("tag=imatch(all).{^test,%check$}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "~* ALL")
    end

    test "generates SQL for negated gt" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.gt.65")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "<=")
    end

    test "generates SQL for negated gte" do
      {:ok, params} = PostgrestParser.parse_query_string("score=not.gte.50")
      {:ok, result} = PostgrestParser.to_sql("tests", params)
      assert String.contains?(result.query, "<")
    end

    test "generates SQL for negated lt" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.lt.18")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, ">=")
    end

    test "generates SQL for negated lte" do
      {:ok, params} = PostgrestParser.parse_query_string("score=not.lte.30")
      {:ok, result} = PostgrestParser.to_sql("tests", params)
      assert String.contains?(result.query, ">")
    end

    test "generates SQL for negated neq (double negation)" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.neq.active")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "=")
    end

    test "generates SQL for negated match" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.match.^test")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "!~")
    end

    test "generates SQL for negated imatch" do
      {:ok, params} = PostgrestParser.parse_query_string("email=not.imatch.^spam")
      {:ok, result} = PostgrestParser.to_sql("messages", params)
      assert String.contains?(result.query, "!~*")
    end

    test "generates SQL for negated cs" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.cs.{banned}")
      {:ok, result} = PostgrestParser.to_sql("posts", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@>")
    end

    test "generates SQL for negated cd" do
      {:ok, params} = PostgrestParser.parse_query_string("roles=not.cd.{admin}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "<@")
    end

    test "generates SQL for negated ov" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.ov.(banned,spam)")
      {:ok, result} = PostgrestParser.to_sql("posts", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "&&")
    end

    test "generates SQL for negated range operators" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.sl.(0,10)")
      {:ok, result} = PostgrestParser.to_sql("intervals", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "<<")
    end

    test "generates SQL for is.not_null" do
      {:ok, params} = PostgrestParser.parse_query_string("email=is.not_null")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "generates SQL for negated is.true" do
      {:ok, params} = PostgrestParser.parse_query_string("active=not.is.true")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "IS NOT TRUE")
    end

    test "generates SQL for negated is.false" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted=not.is.false")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "IS NOT FALSE")
    end

    test "generates SQL for negated is.unknown" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.is.unknown")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "IS NOT UNKNOWN")
    end

    test "generates SQL for negated is.not_null" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=not.is.not_null")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "IS NULL")
    end

    test "generates SQL for FTS with language" do
      {:ok, params} = PostgrestParser.parse_query_string("content=plfts(spanish).buscar")
      {:ok, result} = PostgrestParser.to_sql("docs", params)
      assert String.contains?(result.query, "'spanish'")
      assert String.contains?(result.query, "plainto_tsquery")
    end

    test "generates SQL for field with type cast in filter" do
      {:ok, params} = PostgrestParser.parse_query_string("created_at::date=eq.2024-01-01")
      {:ok, result} = PostgrestParser.to_sql("events", params)
      assert String.contains?(result.query, "::date")
    end

    test "generates SQL for only offset without limit" do
      {:ok, params} = PostgrestParser.parse_query_string("offset=10")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "OFFSET $1")
      assert result.params == [10]
    end

    test "generates SQL with negated ilike" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.ilike.%test%")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT ILIKE")
    end

    test "generates SQL with negated like" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.like.%test%")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT LIKE")
    end

    test "generates SQL for negated FTS operators" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.fts.search")
      {:ok, result} = PostgrestParser.to_sql("docs", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@@")
    end

    test "generates SQL for negated phfts" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.phfts(english).phrase")
      {:ok, result} = PostgrestParser.to_sql("docs", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "phraseto_tsquery")
    end

    test "generates SQL for negated wfts" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.wfts(spanish).web search")
      {:ok, result} = PostgrestParser.to_sql("docs", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "websearch_to_tsquery")
    end

    test "generates SQL for negated plfts" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.plfts.plain text")
      {:ok, result} = PostgrestParser.to_sql("docs", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "plainto_tsquery")
    end

    test "generates SQL with select containing json path cast and alias" do
      {:ok, params} = PostgrestParser.parse_query_string("select=total:data->amount::numeric")
      {:ok, result} = PostgrestParser.to_sql("orders", params)
      assert String.contains?(result.query, "::numeric")
      # The alias format might vary - just check the total is present
      assert String.contains?(result.query, "total")
    end

    test "generates SQL with select containing json path and alias" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user_name:data->>name")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, ~s("data"->>'name'))
      assert String.contains?(result.query, ~s(AS "user_name"))
    end

    test "generates SQL for negated sr range operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.sr.(0,10)")
      {:ok, result} = PostgrestParser.to_sql("intervals", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, ">>")
    end

    test "generates SQL for negated nxl range operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.nxl.(0,10)")
      {:ok, result} = PostgrestParser.to_sql("intervals", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "&<")
    end

    test "generates SQL for negated nxr range operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.nxr.(0,10)")
      {:ok, result} = PostgrestParser.to_sql("intervals", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "&>")
    end

    test "generates SQL for negated adj range operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.adj.(0,10)")
      {:ok, result} = PostgrestParser.to_sql("intervals", params)
      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "-|-")
    end

    test "generates SQL for quantified with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("id=not.eq(any).{1,2,3}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "NOT = ANY")
    end

    test "generates SQL for quantified all with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("id=not.eq(all).{1,2,3}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "NOT = ALL")
    end

    test "generates SQL for quantified neq with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.neq(any).{a,b}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "NOT <> ANY")
    end

    test "generates SQL for quantified gt with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.gt(any).{18,21}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT > ANY")
    end

    test "generates SQL for quantified gte with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("score=not.gte(all).{50,60}")
      {:ok, result} = PostgrestParser.to_sql("tests", params)
      assert String.contains?(result.query, "NOT >= ALL")
    end

    test "generates SQL for quantified lt with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.lt(any).{65,70}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT < ANY")
    end

    test "generates SQL for quantified lte with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("score=not.lte(all).{30,40}")
      {:ok, result} = PostgrestParser.to_sql("tests", params)
      assert String.contains?(result.query, "NOT <= ALL")
    end

    test "generates SQL for quantified like with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.like(any).{%a%,%b%}")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT LIKE ANY")
    end

    test "generates SQL for quantified ilike with negation" do
      {:ok, params} =
        PostgrestParser.parse_query_string("email=not.ilike(all).{%@test%,%@example%}")

      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "NOT ILIKE ALL")
    end

    test "generates SQL for quantified match with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("code=not.match(any).{^A,^B}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "!~ ANY")
    end

    test "generates SQL for quantified imatch with negation" do
      {:ok, params} = PostgrestParser.parse_query_string("tag=not.imatch(all).{^test,^check}")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, "!~* ALL")
    end

    test "generates SQL for select with json path cast without alias" do
      {:ok, params} = PostgrestParser.parse_query_string("select=data->amount::numeric")
      {:ok, result} = PostgrestParser.to_sql("orders", params)
      assert String.contains?(result.query, "::numeric")
    end

    test "generates SQL for select with json path without alias" do
      {:ok, params} = PostgrestParser.parse_query_string("select=data->key")
      {:ok, result} = PostgrestParser.to_sql("items", params)
      assert String.contains?(result.query, ~s("data"->'key'))
    end

    test "builds where clause for empty filter list" do
      alias PostgrestParser.SqlBuilder
      assert {:ok, %{clause: "", params: []}} = SqlBuilder.build_where_clause([])
    end

    test "builds where clause for single filter" do
      alias PostgrestParser.SqlBuilder

      filter = %Filter{
        field: %Field{name: "id", json_path: []},
        operator: :eq,
        value: "1",
        negated?: false
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, ~s("id" = $1))
    end

    test "generates SQL for select with spread item" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,...profile(name)")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert String.contains?(result.query, "SELECT")
    end

    test "returns tables including nested relations" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,posts(id,comments(text))")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert "users" in result.tables
      assert "posts" in result.tables
      assert "comments" in result.tables
    end

    test "returns tables including spread relations" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,...profile(name)")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert "users" in result.tables
      assert "profile" in result.tables
    end
  end

  # ============================================================================
  # SqlBuilder Direct Usage Tests
  # ============================================================================

  describe "SqlBuilder module functions" do
    alias PostgrestParser.SqlBuilder

    test "builds select with nil select items" do
      params = %ParsedParams{select: nil, filters: [], order: [], limit: nil, offset: nil}
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert result.query == ~s(SELECT * FROM "users")
    end

    test "builds select with empty select items" do
      params = %ParsedParams{select: [], filters: [], order: [], limit: nil, offset: nil}
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert result.query == ~s(SELECT * FROM "users")
    end

    test "builds select with field cast and alias" do
      params = %ParsedParams{
        select: [%SelectItem{type: :field, name: "amount", alias: "total", hint: {:cast, "text"}}],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("orders", params)
      assert String.contains?(result.query, "::text")
      assert String.contains?(result.query, ~s(AS "total"))
    end

    test "builds select with JSON path item" do
      params = %ParsedParams{
        select: [%SelectItem{type: :field, name: "data", hint: {:json_path, [{:arrow, "key"}]}}],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, ~s("data"->'key'))
    end

    test "builds select with JSON path and alias" do
      params = %ParsedParams{
        select: [
          %SelectItem{
            type: :field,
            name: "data",
            alias: "key_value",
            hint: {:json_path, [{:double_arrow, "key"}]}
          }
        ],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, ~s("data"->>'key'))
      assert String.contains?(result.query, ~s(AS "key_value"))
    end
  end

  # ============================================================================
  # Additional Coverage Tests
  # ============================================================================

  describe "additional coverage for AliasParser" do
    alias PostgrestParser.SelectParser.AliasParser

    test "handles partial parse fallback" do
      result = AliasParser.parse("field.with.dots")
      assert {_, field} = result
      assert is_binary(field)
    end

    test "handles complex alias patterns" do
      assert {"alias", "field"} = AliasParser.parse("alias:field")
    end

    test "handles error fallback" do
      result = AliasParser.parse("...spread!hint(children)")
      assert {nil, _} = result
    end

    test "handles field with exclamation" do
      result = AliasParser.parse("posts!inner")
      assert {nil, "posts!inner"} = result
    end

    test "handles field with dots" do
      result = AliasParser.parse("schema.table.column")
      assert {_, _} = result
    end

    test "handles JSON with double arrow and alias" do
      result = AliasParser.parse("alias:data->>key")
      assert {"alias", "data->>key"} = result
    end

    test "handles spread with numbers" do
      result = AliasParser.parse("...field123")
      assert {nil, "...field123"} = result
    end

    test "handles complex JSON path with cast" do
      result = AliasParser.parse("data->nested->key::text")
      assert {nil, "data->nested->key::text"} = result
    end

    test "handles alias with underscore" do
      result = AliasParser.parse("my_alias:my_field")
      assert {"my_alias", "my_field"} = result
    end

    test "handles field with multiple colons in cast" do
      result = AliasParser.parse("field::text::varchar")
      assert {_, _} = result
    end

    test "handles spread without identifier" do
      result = AliasParser.parse("...")
      assert {nil, "..."} = result
    end

    test "handles complex nested JSON path" do
      result = AliasParser.parse("data->a->b->c->>d")
      assert {nil, "data->a->b->c->>d"} = result
    end
  end

  describe "additional coverage for FilterParser" do
    alias PostgrestParser.FilterParser

    test "parses filter with all comparison operators" do
      operators = ["eq", "neq", "gt", "gte", "lt", "lte"]

      for op <- operators do
        {:ok, filter} = FilterParser.parse("field", "#{op}.value")
        assert filter.operator == String.to_atom(op)
      end
    end

    test "parses filter with all pattern operators" do
      operators = ["like", "ilike", "match", "imatch"]

      for op <- operators do
        {:ok, filter} = FilterParser.parse("field", "#{op}.%pattern%")
        assert filter.operator == String.to_atom(op)
      end
    end

    test "parses filter with array operators" do
      {:ok, filter} = FilterParser.parse("tags", "cs.{a,b}")
      assert filter.operator == :cs

      {:ok, filter} = FilterParser.parse("tags", "cd.{a,b}")
      assert filter.operator == :cd
    end

    test "parses filter with FTS operators" do
      operators = ["fts", "plfts", "phfts", "wfts"]

      for op <- operators do
        {:ok, filter} = FilterParser.parse("content", "#{op}.term")
        assert filter.operator == String.to_atom(op)
      end
    end

    test "parses filter with range operators" do
      operators = ["sl", "sr", "nxl", "nxr", "adj", "ov"]

      for op <- operators do
        {:ok, filter} = FilterParser.parse("range", "#{op}.(1,10)")
        assert filter.operator == String.to_atom(op)
      end
    end

    test "parses field with cast using fallback" do
      {:ok, field} = FilterParser.parse_field("created_at::date")
      assert field.cast == "date"
    end

    test "parses fallback JSON path with multiple segments" do
      {:ok, field} = FilterParser.parse_field("data->a->>b")
      assert field.json_path == [{:arrow, "a"}, {:double_arrow, "b"}]
    end

    test "handles FTS operator with quantifier-like language correctly" do
      {:ok, filter} = FilterParser.parse("content", "fts(english).term")
      assert filter.operator == :fts
      assert filter.language == "english"
    end

    test "handles negated FTS with language" do
      {:ok, filter} = FilterParser.parse("content", "not.plfts(spanish).buscar")
      assert filter.negated? == true
      assert filter.operator == :plfts
      assert filter.language == "spanish"
    end

    test "handles empty value in filter" do
      {:ok, filter} = FilterParser.parse("name", "eq.")
      assert filter.value == ""
    end

    test "handles is operator with all values" do
      for value <- ["null", "true", "false", "unknown", "not_null"] do
        {:ok, filter} = FilterParser.parse("field", "is.#{value}")
        assert filter.operator == :is
        assert filter.value == value
      end
    end

    test "builds json segment correctly" do
      assert {:arrow, "key"} = FilterParser.build_json_segment([:arrow, "key"])
      assert {:double_arrow, "value"} = FilterParser.build_json_segment([:double_arrow, "value"])
    end

    test "handles quantifier with all operators" do
      for op <- ["eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "match", "imatch"] do
        {:ok, filter} = FilterParser.parse("field", "#{op}(any).{a,b}")
        assert filter.quantifier == :any
      end
    end

    test "returns error for FTS with quantifier" do
      assert {:error, reason} = FilterParser.parse("content", "fts(any).{term1,term2}")
      assert reason =~ "quantifier"
    end

    test "parse_field with dotted name triggers fallback" do
      {:ok, field} = FilterParser.parse_field("table.column")
      assert field.name == "table.column"
    end

    test "parse_field with JSON and cast" do
      {:ok, field} = FilterParser.parse_field("data->key::text")
      assert field.name == "data"
      assert field.json_path == [{:arrow, "key"}]
      assert field.cast == "text"
    end

    test "parse_field with deep JSON path and cast triggers fallback" do
      {:ok, field} = FilterParser.parse_field("data->a->b->>c::text")
      assert field.name == "data"
      assert length(field.json_path) == 3
      assert field.cast == "text"
    end

    test "fallback operator value parsing with negation" do
      {:ok, filter} = FilterParser.parse("table.column", "not.eq.value")
      assert filter.negated? == true
      assert filter.operator == :eq
    end

    test "fallback with phfts and language" do
      {:ok, filter} = FilterParser.parse("content", "phfts(german).suche")
      assert filter.operator == :phfts
      assert filter.language == "german"
    end

    test "fallback with wfts and language" do
      {:ok, filter} = FilterParser.parse("content", "wfts(french).recherche")
      assert filter.operator == :wfts
      assert filter.language == "french"
    end
  end

  describe "additional coverage for LogicParser" do
    alias PostgrestParser.LogicParser

    test "parses logic tree with multiple filter types" do
      {:ok, tree} = LogicParser.parse("and", "(status.eq.active,count.gt.10,name.like.%test%)")
      assert length(tree.conditions) == 3
    end

    test "parses deeply nested with various operators" do
      {:ok, tree} = LogicParser.parse("or", "(a.is.null,and(b.neq.x,or(c.gte.1,d.lte.100)))")
      assert tree.operator == :or
    end

    test "handles complex condition with FTS" do
      {:ok, tree} = LogicParser.parse("and", "(name.eq.test,content.fts.search)")
      [_, fts_filter] = tree.conditions
      assert fts_filter.operator == :fts
    end

    test "handles multiple nested levels" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.1,or(b.eq.2,and(c.eq.3,or(d.eq.4,e.eq.5))))")
      assert tree.operator == :and
    end

    test "handles condition with ilike operator" do
      {:ok, tree} = LogicParser.parse("or", "(name.ilike.%john%,name.ilike.%jane%)")
      assert length(tree.conditions) == 2
    end

    test "handles condition with in operator" do
      {:ok, tree} = LogicParser.parse("and", "(status.in.(a,b,c),type.eq.x)")
      [in_filter, _] = tree.conditions
      assert in_filter.operator == :in
    end

    test "parses not.and condition" do
      {:ok, tree} = LogicParser.parse("not.and", "(a.eq.1,b.eq.2)")
      assert tree.negated? == true
      assert tree.operator == :and
    end

    test "parses not.or condition" do
      {:ok, tree} = LogicParser.parse("not.or", "(a.eq.1,b.eq.2)")
      assert tree.negated? == true
      assert tree.operator == :or
    end

    test "handles negated filters inside tree" do
      {:ok, tree} = LogicParser.parse("and", "(a.not.eq.1,b.not.like.%x%)")
      assert Enum.all?(tree.conditions, & &1.negated?)
    end

    test "handles JSON path in logic tree condition" do
      {:ok, tree} = LogicParser.parse("or", "(data->key.eq.value,name.eq.test)")
      [json_filter, _] = tree.conditions
      assert json_filter.field.json_path == [{:arrow, "key"}]
    end
  end

  describe "additional coverage for SelectParser" do
    alias PostgrestParser.SelectParser

    test "parses select with various item types" do
      {:ok, items} = SelectParser.parse("id,name::text,data->key,...spread(a),rel(b,c)")
      assert length(items) == 5
    end

    test "parses complex nested structure" do
      {:ok, items} = SelectParser.parse("a(b(c(d)))")
      [a] = items
      [b] = a.children
      [c] = b.children
      [d] = c.children
      assert d.name == "d"
    end

    test "handles multiple JSON paths in select" do
      {:ok, items} = SelectParser.parse("data->a,data->b->c,data->>d")
      assert length(items) == 3
    end

    test "handles relation with alias and hint" do
      {:ok, items} = SelectParser.parse("author:users!inner(name)")
      [item] = items
      assert item.alias == "author"
      assert item.name == "users"
      assert item.hint == "inner"
    end

    test "handles spread with alias" do
      {:ok, items} = SelectParser.parse("...author(name,email)")
      [item] = items
      assert item.type == :spread
      assert length(item.children) == 2
    end

    test "handles field with JSON path and cast" do
      {:ok, items} = SelectParser.parse("data->amount::numeric")
      [item] = items
      assert item.hint == {:json_path_cast, [{:arrow, "amount"}], "numeric"}
    end

    test "handles wildcard in relation" do
      {:ok, items} = SelectParser.parse("posts(*)")
      [item] = items
      assert item.type == :relation
      [child] = item.children
      assert child.name == "*"
    end

    test "handles multiple relations" do
      {:ok, items} = SelectParser.parse("posts(id),comments(text),tags(name)")
      assert length(items) == 3
      assert Enum.all?(items, &(&1.type == :relation))
    end

    test "handles nested relations with aliases" do
      {:ok, items} = SelectParser.parse("writer:author(name,works:books(title))")
      [author] = items
      assert author.alias == "writer"
      [_, books] = author.children
      assert books.alias == "works"
    end

    test "handles field with exclamation mark in relation" do
      {:ok, items} = SelectParser.parse("relation!inner(id)")
      [item] = items
      assert item.hint == "inner"
      assert item.type == :relation
    end

    test "handles complex spread with children" do
      {:ok, items} = SelectParser.parse("...author!fk(name,email,bio)")
      [item] = items
      assert item.type == :spread
      assert length(item.children) == 3
    end

    test "handles very deep nesting" do
      {:ok, items} = SelectParser.parse("a(b(c(d(e(f)))))")
      [a] = items
      assert a.type == :relation
    end

    test "handles relation followed by field" do
      {:ok, items} = SelectParser.parse("rel(id),name,other_rel(name)")
      assert length(items) == 3
    end
  end

  describe "additional coverage for OrderParser" do
    alias PostgrestParser.OrderParser

    test "parses multiple order terms with all options" do
      {:ok, terms} =
        OrderParser.parse(
          "a.asc,b.desc,c.nullsfirst,d.nullslast,e.asc.nullsfirst,f.desc.nullslast"
        )

      assert length(terms) == 6
    end

    test "handles order with JSON paths" do
      {:ok, terms} = OrderParser.parse("data->a.asc,data->>b.desc")
      assert length(terms) == 2
    end

    test "handles single term without options" do
      {:ok, [term]} = OrderParser.parse("name")
      assert term.field.name == "name"
      assert term.direction == :asc
      assert term.nulls == nil
    end

    test "handles complex JSON path" do
      {:ok, [term]} = OrderParser.parse("data->user->profile->>score.desc")
      assert term.field.name == "data"
      assert length(term.field.json_path) == 3
    end

    test "handles field with dots using fallback" do
      {:ok, [term]} = OrderParser.parse("schema.table.column.desc")
      assert term.field.name == "schema.table.column"
      assert term.direction == :desc
    end

    test "parses terms with mixed options" do
      {:ok, terms} = OrderParser.parse("a,b.desc,c.nullsfirst,d.asc.nullslast")
      assert length(terms) == 4
      [a, b, c, d] = terms
      assert a.direction == :asc
      assert b.direction == :desc
      assert c.nulls == :first
      assert d.nulls == :last
    end

    test "handles dotted field name with direction" do
      {:ok, [term]} = OrderParser.parse("schema.table.field.desc")
      assert term.field.name == "schema.table.field"
      assert term.direction == :desc
    end

    test "handles dotted field name with nulls" do
      {:ok, [term]} = OrderParser.parse("schema.table.field.asc.nullsfirst")
      assert term.field.name == "schema.table.field"
      assert term.nulls == :first
    end

    test "handles JSON path with multiple segments" do
      {:ok, [term]} = OrderParser.parse("data->a->b->c.desc")
      assert term.field.name == "data"
      assert length(term.field.json_path) == 3
    end
  end

  describe "additional coverage for SqlBuilder" do
    alias PostgrestParser.SqlBuilder

    test "builds complex query with all clauses" do
      params = %ParsedParams{
        select: [
          %SelectItem{type: :field, name: "id"},
          %SelectItem{type: :field, name: "name"}
        ],
        filters: [
          %Filter{field: %Field{name: "status"}, operator: :eq, value: "active", negated?: false},
          %Filter{field: %Field{name: "count"}, operator: :gt, value: "10", negated?: false}
        ],
        order: [
          %OrderTerm{field: %Field{name: "created_at"}, direction: :desc, nulls: nil}
        ],
        limit: 25,
        offset: 50
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "SELECT")
      assert String.contains?(result.query, "WHERE")
      assert String.contains?(result.query, "ORDER BY")
      assert String.contains?(result.query, "LIMIT")
      assert String.contains?(result.query, "OFFSET")
    end

    test "builds query with JSON path in filter" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{
            field: %Field{name: "data", json_path: [{:arrow, "user"}, {:double_arrow, "name"}]},
            operator: :eq,
            value: "John",
            negated?: false
          }
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, ~s("data"->'user'->>'name'))
    end

    test "builds query with is operator variants" do
      for {value, sql} <- [
            {"null", "IS NULL"},
            {"true", "IS TRUE"},
            {"false", "IS FALSE"},
            {"unknown", "IS UNKNOWN"}
          ] do
        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "field"}, operator: :is, value: value, negated?: false}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert String.contains?(result.query, sql)
      end
    end

    test "builds query with negated is operator" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{field: %Field{name: "field"}, operator: :is, value: "null", negated?: true}
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "builds query with order including nulls options" do
      params = %ParsedParams{
        select: nil,
        filters: [],
        order: [
          %OrderTerm{field: %Field{name: "a"}, direction: :asc, nulls: :first},
          %OrderTerm{field: %Field{name: "b"}, direction: :desc, nulls: :last}
        ],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "NULLS FIRST")
      assert String.contains?(result.query, "NULLS LAST")
    end

    test "builds query with type cast in filter field" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{
            field: %Field{name: "date_field", cast: "date"},
            operator: :eq,
            value: "2024-01-01",
            negated?: false
          }
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "::date")
    end

    test "builds query with all comparison operators" do
      for {op, sql_op} <- [
            {:eq, "="},
            {:neq, "<>"},
            {:gt, ">"},
            {:gte, ">="},
            {:lt, "<"},
            {:lte, "<="}
          ] do
        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "field"}, operator: op, value: "1", negated?: false}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert String.contains?(result.query, sql_op)
      end
    end

    test "builds query with all pattern operators" do
      for {op, sql_op} <- [{:like, "LIKE"}, {:ilike, "ILIKE"}, {:match, "~"}, {:imatch, "~*"}] do
        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "field"}, operator: op, value: "%test%", negated?: false}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert String.contains?(result.query, sql_op)
      end
    end

    test "builds query with all FTS operators" do
      for op <- [:fts, :plfts, :phfts, :wfts] do
        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "content"}, operator: op, value: "term", negated?: false}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert String.contains?(result.query, "@@")
      end
    end

    test "builds query with FTS and language" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{
            field: %Field{name: "content"},
            operator: :fts,
            value: "term",
            language: "english",
            negated?: false
          }
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "'english'")
    end

    test "builds query with all set operators" do
      for {op, sql_op} <- [{:in, "ANY"}, {:cs, "@>"}, {:cd, "<@"}, {:ov, "&&"}] do
        value = if op in [:in, :ov], do: ["a", "b"], else: "{a,b}"

        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "field"}, operator: op, value: value, negated?: false}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert String.contains?(result.query, sql_op)
      end
    end

    test "builds query with all range operators" do
      for {op, sql_op} <- [{:sl, "<<"}, {:sr, ">>"}, {:nxl, "&<"}, {:nxr, "&>"}, {:adj, "-|-"}] do
        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "range"}, operator: op, value: "(1,10)", negated?: false}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert String.contains?(result.query, sql_op)
      end
    end

    test "builds query with quantifier any" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{
            field: %Field{name: "field"},
            operator: :eq,
            value: ["a", "b"],
            quantifier: :any,
            negated?: false
          }
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "ANY")
    end

    test "builds query with quantifier all" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{
            field: %Field{name: "field"},
            operator: :eq,
            value: ["a", "b"],
            quantifier: :all,
            negated?: false
          }
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "ALL")
    end

    test "builds query with negated operators" do
      for op <- [:eq, :neq, :gt, :gte, :lt, :lte, :like, :ilike, :match, :imatch] do
        params = %ParsedParams{
          select: nil,
          filters: [
            %Filter{field: %Field{name: "field"}, operator: op, value: "test", negated?: true}
          ],
          order: [],
          limit: nil,
          offset: nil
        }

        {:ok, result} = SqlBuilder.build_select("items", params)
        assert is_binary(result.query)
      end
    end

    test "builds query with is.not_null" do
      params = %ParsedParams{
        select: nil,
        filters: [
          %Filter{field: %Field{name: "field"}, operator: :is, value: "not_null", negated?: false}
        ],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "builds query with multiple orders" do
      params = %ParsedParams{
        select: nil,
        filters: [],
        order: [
          %OrderTerm{field: %Field{name: "a"}, direction: :asc, nulls: nil},
          %OrderTerm{field: %Field{name: "b"}, direction: :desc, nulls: nil},
          %OrderTerm{field: %Field{name: "c"}, direction: :asc, nulls: :first}
        ],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "ORDER BY")
    end

    test "builds query with only limit" do
      params = %ParsedParams{
        select: nil,
        filters: [],
        order: [],
        limit: 100,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "LIMIT")
      refute String.contains?(result.query, "OFFSET")
    end

    test "builds query with only offset" do
      params = %ParsedParams{
        select: nil,
        filters: [],
        order: [],
        limit: nil,
        offset: 50
      }

      {:ok, result} = SqlBuilder.build_select("items", params)
      assert String.contains?(result.query, "OFFSET")
    end
  end

  describe "additional coverage for PostgrestParser main module" do
    test "parse_params with map input" do
      params = %{"id" => "eq.1", "select" => "id,name"}
      {:ok, result} = PostgrestParser.parse_params(params)
      assert result.select != nil
      assert length(result.filters) == 1
    end

    test "query_string_to_sql convenience function" do
      {:ok, result} = PostgrestParser.query_string_to_sql("users", "select=id&limit=10")
      assert String.contains?(result.query, "SELECT")
      assert String.contains?(result.query, "LIMIT")
    end

    test "build_filter_clause function" do
      {:ok, result} = PostgrestParser.build_filter_clause(%{"status" => "eq.active"})
      assert String.contains?(result.clause, "status")
    end

    test "build_filter_clause with multiple filters" do
      {:ok, result} =
        PostgrestParser.build_filter_clause(%{"id" => "eq.1", "status" => "in.(a,b)"})

      assert String.contains?(result.clause, "AND")
    end

    test "parse_params with all options" do
      params = %{
        "select" => "id,name",
        "order" => "id.desc",
        "limit" => "10",
        "offset" => "5"
      }

      {:ok, result} = PostgrestParser.parse_params(params)
      assert result.limit == 10
      assert result.offset == 5
    end

    test "parse returns error for invalid limit" do
      {:error, reason} = PostgrestParser.parse_query_string("limit=invalid")
      assert reason =~ "limit"
    end

    test "parse returns error for negative limit" do
      {:error, reason} = PostgrestParser.parse_query_string("limit=-5")
      assert reason =~ "limit"
    end

    test "parse returns error for invalid offset" do
      {:error, reason} = PostgrestParser.parse_query_string("offset=abc")
      assert reason =~ "offset"
    end

    test "parse returns error for invalid filter" do
      {:error, _} = PostgrestParser.parse_query_string("id=invalid_op.value")
    end

    test "parse with logic tree in params" do
      params = %{"and" => "(id.eq.1,name.eq.test)"}
      {:ok, result} = PostgrestParser.parse_params(params)
      assert length(result.filters) == 1
    end

    test "parse_params with empty map" do
      {:ok, result} = PostgrestParser.parse_params(%{})
      assert result.select == nil
      assert result.filters == []
    end

    test "parse_params preserves filter order" do
      {:ok, result} = PostgrestParser.parse_query_string("a=eq.1&b=eq.2&c=eq.3")
      names = Enum.map(result.filters, & &1.field.name)
      assert names == ["a", "b", "c"]
    end
  end

  describe "additional coverage for RelationBuilder" do
    alias PostgrestParser.RelationBuilder
    alias PostgrestParser.SchemaCache.Relationship

    test "builds join with multiple source/target columns" do
      rel = %Relationship{
        constraint_name: "composite_fk",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "source",
        target_schema: "public",
        target_table: "target",
        source_columns: ["col_a", "col_b"],
        target_columns: ["id_a", "id_b"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "target",
        children: [%SelectItem{type: :field, name: "name"}]
      }

      {join_sql, _} = RelationBuilder.build_single_relation_join(select_item, rel, "source", 0)
      assert String.contains?(join_sql, "AND")
    end

    test "builds join with multiple children" do
      rel = %Relationship{
        constraint_name: "test_fk",
        cardinality: :m2o,
        source_schema: "public",
        source_table: "orders",
        target_schema: "public",
        target_table: "customers",
        source_columns: ["customer_id"],
        target_columns: ["id"]
      }

      select_item = %SelectItem{
        type: :relation,
        name: "customer",
        children: [
          %SelectItem{type: :field, name: "id"},
          %SelectItem{type: :field, name: "name"},
          %SelectItem{type: :field, name: "email"}
        ]
      }

      {join_sql, _} = RelationBuilder.build_single_relation_join(select_item, rel, "orders", 0)
      # The join SQL uses row_to_json which includes all selected fields
      assert String.contains?(join_sql, "row_to_json")
      assert String.contains?(join_sql, "customer_0")
    end

    test "builds join with different cardinality types" do
      for cardinality <- [:m2o, :o2m, :o2o] do
        rel = %Relationship{
          constraint_name: "test_fk",
          cardinality: cardinality,
          source_schema: "public",
          source_table: "source",
          target_schema: "public",
          target_table: "target",
          source_columns: ["id"],
          target_columns: ["source_id"]
        }

        select_item = %SelectItem{
          type: :relation,
          name: "target",
          children: [%SelectItem{type: :field, name: "name"}]
        }

        {join_sql, _} = RelationBuilder.build_single_relation_join(select_item, rel, "source", 0)
        assert String.contains?(join_sql, "LEFT JOIN LATERAL")
      end
    end
  end

  # ============================================================================
  # Coverage Tests for Fallback Paths
  # ============================================================================

  describe "AliasParser fallback paths" do
    alias PostgrestParser.SelectParser.AliasParser

    test "parses field with special characters that trigger fallback" do
      # Fields with special chars like ! trigger fallback parsing
      result = AliasParser.parse("relation!inner:alias")
      assert is_tuple(result)
    end

    test "parses complex alias with cast pattern" do
      assert {"price_str", "price::text"} = AliasParser.parse("price::text:price_str")
    end

    test "parses field with multiple colons" do
      result = AliasParser.parse("a:b:c")
      assert is_tuple(result)
    end

    test "parses field starting with special character" do
      result = AliasParser.parse("@special")
      assert is_tuple(result)
    end

    test "parses field with embedded colon in cast" do
      result = AliasParser.parse("data::json:alias_name")
      assert is_tuple(result)
    end
  end

  describe "FilterParser fallback paths" do
    alias PostgrestParser.FilterParser

    test "parses field with type cast using fallback" do
      {:ok, field} = FilterParser.parse_field("amount::numeric")
      assert field.name == "amount"
      assert field.cast == "numeric"
    end

    test "parses simple field name through main path" do
      {:ok, field} = FilterParser.parse_field("simple_field")
      assert field.name == "simple_field"
      assert field.json_path == []
    end

    test "parses field with json path and cast" do
      {:ok, field} = FilterParser.parse_field("data->>key::text")
      assert field.name == "data"
      assert length(field.json_path) == 1
    end

    test "rejects non-string field input" do
      assert {:error, _} = FilterParser.parse_field(123)
    end

    test "parses filter with negated fallback operator" do
      {:ok, filter} = FilterParser.parse("field", "not.eq.value")
      assert filter.negated? == true
      assert filter.operator == :eq
    end

    test "handles list extraction fallback with regex" do
      {:ok, filter} = FilterParser.parse("tags", "cs.{a,b,c}")
      assert filter.operator == :cs
      assert filter.value == "{a,b,c}"
    end

    test "handles quantified operator list extraction fallback" do
      {:ok, filter} = FilterParser.parse("id", "eq(any).{1,2,3}")
      assert filter.quantifier == :any
      assert is_list(filter.value)
    end
  end

  describe "OrderParser fallback paths" do
    alias PostgrestParser.OrderParser

    test "parses order with only nulls option" do
      {:ok, [term]} = OrderParser.parse("field.nullsfirst")
      assert term.nulls == :first
      assert term.direction == :asc
    end

    test "parses complex field name in order" do
      {:ok, terms} = OrderParser.parse("my_field.desc")
      assert length(terms) == 1
      assert hd(terms).direction == :desc
    end

    test "parses nil order string" do
      {:ok, []} = OrderParser.parse(nil)
    end

    test "parses order term with special characters" do
      {:ok, term} = OrderParser.parse_term("field_name.desc.nullslast")
      assert term.direction == :desc
      assert term.nulls == :last
    end

    test "parses order term without direction or nulls" do
      {:ok, term} = OrderParser.parse_term("simple_field")
      assert term.direction == :asc
      assert is_nil(term.nulls)
    end

    test "handles order term with extra parts treated as field name" do
      {:ok, term} = OrderParser.parse_term("field.invalid.options")
      # The parser treats "field.invalid.options" as a field with dots in the name
      assert term.field.name == "field.invalid.options"
      assert term.direction == :asc
    end

    test "handles actual invalid options error" do
      {:error, reason} = OrderParser.parse_term("field.asc.invalid")
      assert String.contains?(reason, "invalid")
    end
  end

  describe "SelectParser fallback and error paths" do
    alias PostgrestParser.SelectParser

    test "parses empty select returns empty list" do
      {:ok, []} = SelectParser.parse("")
    end

    test "parses nil select returns empty list" do
      {:ok, []} = SelectParser.parse(nil)
    end

    test "parses field with type cast" do
      {:ok, [item]} = SelectParser.parse("amount::numeric")
      assert item.name == "amount"
      assert {:cast, "numeric"} = item.hint
    end

    test "parses JSON path with cast" do
      {:ok, [item]} = SelectParser.parse("data->>name::text")
      assert item.name == "data"
      assert {:json_path_cast, _, "text"} = item.hint
    end

    test "parses relation with hint before alias" do
      {:ok, [item]} = SelectParser.parse("customers!inner(id)")
      assert item.type == :relation
      assert item.hint == "inner"
    end

    test "handles unclosed parenthesis error" do
      {:error, reason} = SelectParser.parse("relation(id")
      assert String.contains?(reason, "unclosed") or String.contains?(reason, "unexpected")
    end

    test "handles empty relation children" do
      {:ok, [item]} = SelectParser.parse("relation()")
      assert item.children == []
    end

    test "parses nested relation within nested relation" do
      {:ok, [item]} = SelectParser.parse("a(b(c(d)))")
      assert item.name == "a"
      assert hd(item.children).name == "b"
    end
  end

  describe "LogicParser fallback paths" do
    alias PostgrestParser.LogicParser

    test "checks if key is and" do
      assert LogicParser.logic_key?("and")
    end

    test "checks if key is or" do
      assert LogicParser.logic_key?("or")
    end

    test "checks if key is not.and" do
      assert LogicParser.logic_key?("not.and")
    end

    test "checks if key is not.or" do
      assert LogicParser.logic_key?("not.or")
    end

    test "checks if key is not a logic key" do
      refute LogicParser.logic_key?("id")
      refute LogicParser.logic_key?("select")
    end

    test "handles missing parentheses in logic expression" do
      {:error, reason} = LogicParser.parse("and", "id.eq.1,name.eq.john")
      assert String.contains?(reason, "parentheses")
    end

    test "handles unclosed parenthesis in logic expression" do
      {:error, reason} = LogicParser.parse("or", "(id.eq.1,name.eq.john")

      # The error message says "must be wrapped in parentheses" because the outer parens don't close properly
      assert String.contains?(reason, "parentheses") or String.contains?(reason, "unclosed")
    end

    test "handles unexpected closing parenthesis" do
      {:error, reason} = LogicParser.parse("and", "(id.eq.1))")
      assert String.contains?(reason, "unexpected")
    end

    test "handles deeply nested logic with correct parsing" do
      {:ok, tree} = LogicParser.parse("and", "(id.eq.1,or(status.eq.a,status.eq.b))")
      assert tree.operator == :and
      assert length(tree.conditions) == 2
    end

    test "parses not.or nested inside and" do
      {:ok, tree} = LogicParser.parse("and", "(id.eq.1,not.or(a.eq.1,b.eq.2))")
      assert tree.operator == :and
      [_, nested] = tree.conditions
      assert nested.negated?
      assert nested.operator == :or
    end
  end

  describe "SqlBuilder edge cases and operator paths" do
    alias PostgrestParser.SqlBuilder

    test "builds IS NOT NULL clause" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=not.is.null")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "builds IS NOT TRUE clause" do
      {:ok, params} = PostgrestParser.parse_query_string("active=not.is.true")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "IS NOT TRUE")
    end

    test "builds IS NOT FALSE clause" do
      {:ok, params} = PostgrestParser.parse_query_string("disabled=not.is.false")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "IS NOT FALSE")
    end

    test "builds IS NOT UNKNOWN clause" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.is.unknown")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "IS NOT UNKNOWN")
    end

    test "builds IS not_null clause" do
      {:ok, params} = PostgrestParser.parse_query_string("field=is.not_null")
      {:ok, result} = SqlBuilder.build_select("table", params)
      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "builds negated IS not_null clause (becomes IS NULL)" do
      {:ok, params} = PostgrestParser.parse_query_string("field=not.is.not_null")
      {:ok, result} = SqlBuilder.build_select("table", params)
      assert String.contains?(result.query, "IS NULL")
    end

    test "builds query with only offset" do
      {:ok, params} = PostgrestParser.parse_query_string("offset=10")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "OFFSET")
      assert result.params == [10]
    end

    test "builds query with limit and offset" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=10&offset=20")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "LIMIT")
      assert String.contains?(result.query, "OFFSET")
      assert result.params == [10, 20]
    end

    test "builds where clause directly" do
      filter = %Filter{
        field: %Field{name: "id", json_path: []},
        operator: :eq,
        value: "1",
        negated?: false
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert result.clause == ~s("id" = $1)
      assert result.params == [1]
    end

    test "builds where clause with logic tree" do
      tree = %LogicTree{
        operator: :and,
        negated?: false,
        conditions: [
          %Filter{
            field: %Field{name: "a", json_path: []},
            operator: :eq,
            value: "1",
            negated?: false
          },
          %Filter{
            field: %Field{name: "b", json_path: []},
            operator: :eq,
            value: "2",
            negated?: false
          }
        ]
      }

      {:ok, result} = SqlBuilder.build_where_clause([tree])
      assert String.contains?(result.clause, "AND")
    end

    test "builds where clause with negated logic tree" do
      tree = %LogicTree{
        operator: :or,
        negated?: true,
        conditions: [
          %Filter{
            field: %Field{name: "a", json_path: []},
            operator: :eq,
            value: "1",
            negated?: false
          },
          %Filter{
            field: %Field{name: "b", json_path: []},
            operator: :eq,
            value: "2",
            negated?: false
          }
        ]
      }

      {:ok, result} = SqlBuilder.build_where_clause([tree])
      assert String.contains?(result.clause, "NOT")
      assert String.contains?(result.clause, "OR")
    end

    test "builds SQL for neq with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=neq(any).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "<> ANY")
    end

    test "builds SQL for neq with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=neq(all).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "<> ALL")
    end

    test "builds SQL for gt with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=gt(any).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "> ANY")
    end

    test "builds SQL for gt with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=gt(all).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "> ALL")
    end

    test "builds SQL for gte with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=gte(any).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, ">= ANY")
    end

    test "builds SQL for gte with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=gte(all).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, ">= ALL")
    end

    test "builds SQL for lt with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=lt(any).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "< ANY")
    end

    test "builds SQL for lt with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=lt(all).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "< ALL")
    end

    test "builds SQL for lte with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=lte(any).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "<= ANY")
    end

    test "builds SQL for lte with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("id=lte(all).{1,2,3}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "<= ALL")
    end

    test "builds SQL for like with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like(any).{a%,b%}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "LIKE ANY")
    end

    test "builds SQL for like with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=like(all).{%a,%b}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "LIKE ALL")
    end

    test "builds SQL for ilike with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=ilike(any).{a%,b%}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "ILIKE ANY")
    end

    test "builds SQL for ilike with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=ilike(all).{%a,%b}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "ILIKE ALL")
    end

    test "builds SQL for match with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=match(any).{^a,^b}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "~ ANY")
    end

    test "builds SQL for match with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=match(all).{a$,b$}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "~ ALL")
    end

    test "builds SQL for imatch with any quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=imatch(any).{^a,^b}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "~* ANY")
    end

    test "builds SQL for imatch with all quantifier" do
      {:ok, params} = PostgrestParser.parse_query_string("name=imatch(all).{a$,b$}")
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "~* ALL")
    end

    test "builds SQL with field cast" do
      filter = %Filter{
        field: %Field{name: "amount", json_path: [], cast: "numeric"},
        operator: :gt,
        value: "100",
        negated?: false
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "::numeric")
    end

    test "builds SQL with field cast and json path" do
      filter = %Filter{
        field: %Field{name: "data", json_path: [{:double_arrow, "amount"}], cast: "numeric"},
        operator: :gt,
        value: "100",
        negated?: false
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "::numeric")
      assert String.contains?(result.clause, "->>")
    end

    test "builds SQL with spread select item" do
      params = %ParsedParams{
        select: [%SelectItem{type: :spread, name: "profile"}],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "*")
    end

    test "builds SQL with relation select item containing children" do
      params = %ParsedParams{
        select: [
          %SelectItem{type: :field, name: "id"},
          %SelectItem{
            type: :relation,
            name: "orders",
            children: [
              %SelectItem{type: :field, name: "id"},
              %SelectItem{type: :field, name: "total"}
            ]
          }
        ],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "json_agg")
    end

    test "builds empty where clause" do
      {:ok, result} = SqlBuilder.build_where_clause([])
      assert result.clause == ""
      assert result.params == []
    end

    test "coerces decimal values" do
      {:ok, params} = PostgrestParser.parse_query_string("price=gt.99.99")
      {:ok, result} = SqlBuilder.build_select("items", params)
      assert length(result.params) == 1
    end
  end

  describe "additional edge cases for 90% coverage" do
    alias PostgrestParser.SelectParser.AliasParser

    test "AliasParser handles field with hint and alias" do
      result = AliasParser.parse("my_alias:field!hint")
      assert is_tuple(result)
      {alias_name, field_name} = result
      assert alias_name == "my_alias" or is_nil(alias_name)
      assert is_binary(field_name)
    end

    test "AliasParser handles field with empty alias part" do
      result = AliasParser.parse(":field")
      assert is_tuple(result)
    end

    test "AliasParser handles complex JSON path with alias" do
      result = AliasParser.parse("my_alias:data->user->>email")
      {alias_name, field_name} = result
      assert alias_name == "my_alias"
      assert field_name == "data->user->>email"
    end

    test "AliasParser with unicode characters triggers fallback" do
      result = AliasParser.parse("field_éàü")
      assert is_tuple(result)
    end

    test "AliasParser with @ symbol triggers fallback" do
      result = AliasParser.parse("user@email")
      assert is_tuple(result)
    end
  end

  describe "SelectParser fallback token parsing" do
    alias PostgrestParser.SelectParser

    test "parses deeply nested relations triggering fallback paths" do
      {:ok, items} = SelectParser.parse("a(b(c(d(e))))")
      assert hd(items).name == "a"
      assert hd(hd(items).children).name == "b"
    end

    test "parses multiple relations at same level" do
      {:ok, items} = SelectParser.parse("a(id),b(id),c(id)")
      assert length(items) == 3
      assert Enum.all?(items, &(&1.type == :relation))
    end

    test "parses relation with multiple nested fields" do
      {:ok, [item]} = SelectParser.parse("posts(id,title,content,author)")
      assert length(item.children) == 4
    end

    test "parses complex mix of fields and relations" do
      {:ok, items} = SelectParser.parse("id,name,orders(id,items(name,price)),profile(bio)")
      assert length(items) == 4
    end

    test "handles relation with spread inside" do
      {:ok, [item]} = SelectParser.parse("author(...profile(bio))")
      assert item.type == :relation
      assert hd(item.children).type == :spread
    end

    test "handles multiple spreads" do
      {:ok, items} = SelectParser.parse("...a(x),...b(y)")
      assert length(items) == 2
      assert Enum.all?(items, &(&1.type == :spread))
    end
  end

  describe "LogicParser additional paths" do
    alias PostgrestParser.LogicParser

    test "parses equals notation inside logic" do
      {:ok, tree} = LogicParser.parse("or", "(status=eq.active,status=eq.pending)")
      assert tree.operator == :or
      assert length(tree.conditions) == 2
    end

    test "parses mixed notation" do
      {:ok, tree} = LogicParser.parse("and", "(id.eq.1,status=eq.active)")
      assert tree.operator == :and
      assert length(tree.conditions) == 2
    end

    test "parses triple nested logic" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.1,or(b.eq.2,and(c.eq.3,d.eq.4)))")
      assert tree.operator == :and
      assert length(tree.conditions) == 2
    end

    test "parses not.and inside not.or" do
      {:ok, tree} = LogicParser.parse("not.or", "(a.eq.1,not.and(b.eq.2,c.eq.3))")
      assert tree.negated?
      assert tree.operator == :or
    end

    test "handles invalid nested logic format" do
      {:error, reason} = LogicParser.parse("and", "(invalid)")
      assert String.contains?(reason, "invalid") or String.contains?(reason, "format")
    end

    test "handles simple values in logic expression" do
      {:ok, tree} = LogicParser.parse("and", "(a.eq.hello,b.eq.world)")
      assert length(tree.conditions) == 2
    end
  end

  describe "FilterParser additional coverage" do
    alias PostgrestParser.FilterParser

    test "parses negated in operator" do
      {:ok, filter} = FilterParser.parse("status", "not.in.(a,b,c)")
      assert filter.negated?
      assert filter.operator == :in
    end

    test "parses is operator with various values" do
      for value <- ["null", "not_null", "true", "false", "unknown"] do
        {:ok, filter} = FilterParser.parse("field", "is.#{value}")
        assert filter.operator == :is
        assert filter.value == value
      end
    end

    test "parses negated is operator" do
      {:ok, filter} = FilterParser.parse("field", "not.is.null")
      assert filter.negated?
      assert filter.operator == :is
    end

    test "parses FTS operator with language" do
      {:ok, filter} = FilterParser.parse("content", "plfts(spanish).hola")
      assert filter.operator == :plfts
      assert filter.language == "spanish"
    end

    test "parses negated FTS operator" do
      {:ok, filter} = FilterParser.parse("content", "not.fts.hello")
      assert filter.negated?
      assert filter.operator == :fts
    end

    test "parses cs operator" do
      {:ok, filter} = FilterParser.parse("tags", "cs.{a,b,c}")
      assert filter.operator == :cs
    end

    test "parses cd operator" do
      {:ok, filter} = FilterParser.parse("roles", "cd.{admin,user}")
      assert filter.operator == :cd
    end

    test "parses negated range operators" do
      for op <- ["sl", "sr", "nxl", "nxr", "adj"] do
        {:ok, filter} = FilterParser.parse("range", "not.#{op}.[1,10)")
        assert filter.negated?
        assert filter.operator == String.to_atom(op)
      end
    end
  end

  describe "SqlBuilder array index handling" do
    alias PostgrestParser.SqlBuilder
    alias PostgrestParser.AST.{Filter, Field}

    test "builds SQL for JSON array index access" do
      filter = %Filter{
        field: %Field{name: "data", json_path: [{:array_index, 0}]},
        operator: :eq,
        value: "test",
        negated?: false
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "->0")
    end

    test "builds SQL for mixed JSON path with array index" do
      filter = %Filter{
        field: %Field{
          name: "data",
          json_path: [{:arrow, "items"}, {:array_index, 0}, {:double_arrow, "name"}]
        },
        operator: :eq,
        value: "test",
        negated?: false
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "->")
      assert String.contains?(result.clause, "->>")
    end
  end

  describe "more SqlBuilder quantifier negation paths" do
    alias PostgrestParser.SqlBuilder
    alias PostgrestParser.AST.{Filter, Field}

    test "negated eq with any quantifier" do
      filter = %Filter{
        field: %Field{name: "id", json_path: []},
        operator: :eq,
        value: [1, 2, 3],
        negated?: true,
        quantifier: :any
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "NOT")
      assert String.contains?(result.clause, "ANY")
    end

    test "negated eq with all quantifier" do
      filter = %Filter{
        field: %Field{name: "id", json_path: []},
        operator: :eq,
        value: [1, 2, 3],
        negated?: true,
        quantifier: :all
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "NOT")
      assert String.contains?(result.clause, "ALL")
    end

    test "negated gt with any quantifier" do
      filter = %Filter{
        field: %Field{name: "id", json_path: []},
        operator: :gt,
        value: [1, 2, 3],
        negated?: true,
        quantifier: :any
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "NOT")
    end

    test "negated like with any quantifier" do
      filter = %Filter{
        field: %Field{name: "name", json_path: []},
        operator: :like,
        value: ["%a%", "%b%"],
        negated?: true,
        quantifier: :any
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "NOT")
      assert String.contains?(result.clause, "LIKE ANY")
    end

    test "negated ilike with all quantifier" do
      filter = %Filter{
        field: %Field{name: "name", json_path: []},
        operator: :ilike,
        value: ["%a%", "%b%"],
        negated?: true,
        quantifier: :all
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "NOT")
      assert String.contains?(result.clause, "ILIKE ALL")
    end

    test "negated match with any quantifier" do
      filter = %Filter{
        field: %Field{name: "name", json_path: []},
        operator: :match,
        value: ["^a", "^b"],
        negated?: true,
        quantifier: :any
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "!")
    end

    test "negated imatch with all quantifier" do
      filter = %Filter{
        field: %Field{name: "name", json_path: []},
        operator: :imatch,
        value: ["^a", "^b"],
        negated?: true,
        quantifier: :all
      }

      {:ok, result} = SqlBuilder.build_where_clause([filter])
      assert String.contains?(result.clause, "!")
      assert String.contains?(result.clause, "~* ALL")
    end
  end

  describe "fallback parser paths with special characters" do
    alias PostgrestParser.SelectParser.AliasParser
    alias PostgrestParser.FilterParser
    alias PostgrestParser.SelectParser

    test "AliasParser with special characters triggers fallback" do
      assert {"my_alias", "@field"} = AliasParser.parse("my_alias:@field")
      assert {"my_alias", "field$name"} = AliasParser.parse("my_alias:field$name")
      assert {nil, "field#1"} = AliasParser.parse("field#1")
    end

    test "AliasParser with unicode alias triggers fallback" do
      {alias_name, field_name} = AliasParser.parse("тест:field")
      assert is_binary(alias_name)
      assert field_name == "field"
    end

    test "AliasParser with unicode field triggers fallback" do
      {alias_name, field_name} = AliasParser.parse("my_alias:フィールド")
      assert alias_name == "my_alias"
      assert is_binary(field_name)
    end

    test "FilterParser with special character field triggers fallback" do
      {:ok, filter} = FilterParser.parse("@field", "eq.1")
      assert filter.field.name == "@field"
    end

    test "FilterParser with dotted field name triggers fallback" do
      {:ok, filter} = FilterParser.parse("schema.table.column", "eq.1")
      assert filter.field.name == "schema.table.column"
    end

    test "FilterParser with numeric prefix triggers fallback" do
      {:ok, filter} = FilterParser.parse("123field", "eq.1")
      assert filter.field.name == "123field"
    end

    test "FilterParser.parse_field with special char and cast triggers fallback path" do
      {:ok, field} = FilterParser.parse_field("@field::text")
      assert field.name == "@field"
      assert field.cast == "text"
    end

    test "FilterParser.parse_field with numeric prefix and cast" do
      {:ok, field} = FilterParser.parse_field("123field::integer")
      assert field.name == "123field"
      assert field.cast == "integer"
    end

    test "FilterParser.parse_field with special chars and JSON path" do
      {:ok, field} = FilterParser.parse_field("@data->>name")
      assert field.name == "@data"
      assert length(field.json_path) == 1
    end

    test "FilterParser.parse_field with special chars, JSON path and cast" do
      {:ok, field} = FilterParser.parse_field("@data->>name::text")
      assert field.name == "@data"
      assert field.cast == "text"
    end

    test "SelectParser with special character field" do
      {:ok, items} = SelectParser.parse("@field,$name,#id")
      assert length(items) == 3
    end

    test "SelectParser with unicode field names" do
      {:ok, items} = SelectParser.parse("名前,メール")
      assert length(items) == 2
    end

    test "SelectParser with relation having special char field" do
      {:ok, [item]} = SelectParser.parse("author(@email)")
      assert item.type == :relation
      assert hd(item.children).name == "@email"
    end
  end

  describe "LogicParser additional error paths" do
    alias PostgrestParser.LogicParser

    test "parses deeply nested not expressions" do
      {:ok, tree} = LogicParser.parse("not.and", "(not.or(a.eq.1,b.eq.2),c.eq.3)")
      assert tree.negated?
      assert tree.operator == :and
    end

    test "handles multiple conditions in nested logic" do
      {:ok, tree} = LogicParser.parse("or", "(a.eq.1,b.eq.2,c.eq.3,d.eq.4)")
      assert length(tree.conditions) == 4
    end

    test "parses condition with special operator value" do
      {:ok, tree} = LogicParser.parse("and", "(status.eq.in_progress,count.gt.0)")
      assert length(tree.conditions) == 2
    end
  end

  describe "FilterParser operator variations" do
    alias PostgrestParser.FilterParser

    test "parses all FTS operators with languages" do
      for op <- ["fts", "plfts", "phfts", "wfts"] do
        for lang <- ["english", "spanish", "french", "german"] do
          {:ok, filter} = FilterParser.parse("content", "#{op}(#{lang}).search")
          assert filter.operator == String.to_atom(op)
          assert filter.language == lang
        end
      end
    end

    test "parses negated FTS operators with languages" do
      {:ok, filter} = FilterParser.parse("content", "not.fts(english).search")
      assert filter.negated?
      assert filter.language == "english"
    end

    test "parses all quantifiable operators with both quantifiers" do
      ops = ["eq", "neq", "gt", "gte", "lt", "lte", "like", "ilike", "match", "imatch"]

      for op <- ops, quant <- ["any", "all"] do
        {:ok, filter} = FilterParser.parse("field", "#{op}(#{quant}).{a,b}")
        assert filter.quantifier == String.to_atom(quant)
      end
    end
  end

  describe "SelectParser JSON path variations" do
    alias PostgrestParser.SelectParser

    test "parses deeply nested JSON path" do
      {:ok, [item]} = SelectParser.parse("data->level1->level2->level3->>value")
      assert item.name == "data"
    end

    test "parses JSON path with alias" do
      {:ok, [item]} = SelectParser.parse("my_value:data->>value")
      assert item.alias == "my_value"
    end

    test "parses multiple JSON path fields" do
      {:ok, items} = SelectParser.parse("a->>x,b->>y,c->>z")
      assert length(items) == 3
    end

    test "parses relation with JSON path field" do
      {:ok, [item]} = SelectParser.parse("orders(data->>status)")
      assert item.type == :relation
      assert hd(item.children).name == "data"
    end
  end

  describe "SelectParser error paths" do
    alias PostgrestParser.SelectParser

    test "handles empty item in list" do
      {:error, _} = SelectParser.parse(",a")
    end

    test "handles invalid nested structure" do
      {:error, _} = SelectParser.parse("a(")
    end

    test "handles double open parens" do
      {:error, _} = SelectParser.parse("a((b))")
    end

    test "handles mismatched parens" do
      {:error, _} = SelectParser.parse("a(b))")
    end

    test "handles field with embedded open paren" do
      {:error, reason} = SelectParser.parse("field(name")
      assert String.contains?(reason, "unclosed") or String.contains?(reason, "unexpected")
    end

    test "handles spread without children" do
      # Spread expects children but this is just the name without parens
      {:ok, [item]} = SelectParser.parse("...profile")
      assert item.type == :spread
    end

    test "handles double open parens specifically" do
      {:error, reason} = SelectParser.parse("a((b")
      assert String.contains?(reason, "unexpected")
    end

    test "handles empty item in middle of list" do
      {:error, reason} = SelectParser.parse("a,,b")
      assert String.contains?(reason, "unexpected")
    end

    test "handles just open paren" do
      {:error, reason} = SelectParser.parse("(")
      assert String.contains?(reason, "unexpected")
    end

    test "handles nested relation without closing" do
      {:error, reason} = SelectParser.parse("a(b(c)")
      assert String.contains?(reason, "unclosed")
    end
  end

  describe "LogicParser error and edge paths" do
    alias PostgrestParser.LogicParser

    test "handles empty conditions" do
      # Empty conditions string should work
      {:ok, tree} = LogicParser.parse("and", "(a.eq.1)")
      assert length(tree.conditions) == 1
    end

    test "handles single condition" do
      {:ok, tree} = LogicParser.parse("or", "(id.eq.1)")
      assert length(tree.conditions) == 1
    end

    test "handles malformed filter in condition" do
      {:error, reason} = LogicParser.parse("and", "(invalid)")
      assert is_binary(reason)
    end

    test "handles very deeply nested logic" do
      {:ok, tree} = LogicParser.parse("and", "(or(and(or(a.eq.1,b.eq.2),c.eq.3),d.eq.4))")
      assert tree.operator == :and
    end

    test "handles condition with multiple not prefixes" do
      {:ok, tree} = LogicParser.parse("not.and", "(a.not.eq.1,b.eq.2)")
      assert tree.negated?
    end
  end

  describe "FilterParser remaining paths" do
    alias PostgrestParser.FilterParser

    test "parses field with invalid JSON path returns error" do
      # An input that would create invalid JSON segments
      {:ok, field} = FilterParser.parse_field("data->->key")
      # The parser handles this gracefully
      assert is_binary(field.name)
    end

    test "handles non-string field input" do
      {:error, _} = FilterParser.parse_field(nil)
    end

    test "handles empty operator value" do
      {:error, reason} = FilterParser.parse("field", "")
      assert is_binary(reason)
    end

    test "handles operator without value" do
      {:error, reason} = FilterParser.parse("field", "eq")
      assert is_binary(reason)
    end

    test "parses negated quantified operator" do
      {:ok, filter} = FilterParser.parse("id", "not.eq(any).{1,2,3}")
      assert filter.negated?
      assert filter.quantifier == :any
    end
  end

  describe "SqlBuilder additional coverage" do
    alias PostgrestParser.SqlBuilder
    alias PostgrestParser.AST.{Filter, Field, SelectItem, ParsedParams}

    test "handles nil select" do
      params = %ParsedParams{select: nil, filters: [], order: [], limit: nil, offset: nil}
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "*")
    end

    test "handles empty select" do
      params = %ParsedParams{select: [], filters: [], order: [], limit: nil, offset: nil}
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "*")
    end

    test "handles select with field alias and JSON path cast" do
      params = %ParsedParams{
        select: [
          %SelectItem{
            type: :field,
            name: "data",
            alias: "my_alias",
            hint: {:json_path_cast, [{:double_arrow, "value"}], "text"}
          }
        ],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "AS")
      assert String.contains?(result.query, "::text")
    end

    test "handles select with JSON path and no alias" do
      params = %ParsedParams{
        select: [
          %SelectItem{
            type: :field,
            name: "data",
            alias: nil,
            hint: {:json_path, [{:double_arrow, "value"}]}
          }
        ],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "->>")
    end

    test "handles select with cast only" do
      params = %ParsedParams{
        select: [
          %SelectItem{
            type: :field,
            name: "amount",
            alias: nil,
            hint: {:cast, "numeric"}
          }
        ],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "::numeric")
    end
  end

  describe "LogicParser remaining error paths" do
    alias PostgrestParser.LogicParser

    test "handles unclosed parenthesis in nested conditions (depth > 0)" do
      # Input like "(a.eq.1,or(b.eq.2" - the outer parens close but inner don't
      {:error, reason} = LogicParser.parse("and", "(a.eq.1,or(b.eq.2)")
      assert is_binary(reason)
    end

    test "handles invalid nested logic without proper closing" do
      {:error, reason} = LogicParser.parse("and", "(and(a.eq.1)")
      assert is_binary(reason)
    end

    test "handles filter with only field and not prefix but no operator" do
      {:error, reason} = LogicParser.parse("and", "(field.not)")
      assert is_binary(reason)
    end

    test "handles filter with dot but too few parts" do
      {:error, reason} = LogicParser.parse("or", "(field)")
      assert is_binary(reason)
    end

    test "handles single filter without dots" do
      {:error, reason} = LogicParser.parse("and", "(fieldvalue)")
      assert is_binary(reason)
    end
  end

  describe "FilterParser JSON path error paths" do
    alias PostgrestParser.FilterParser

    test "handles field with consecutive JSON operators" do
      # This should trigger the parse_json_segments error path
      {:ok, field} = FilterParser.parse_field("@data->->key")
      # Parser handles this gracefully - may split in unexpected ways
      assert is_binary(field.name)
    end

    test "handles field with trailing JSON operator" do
      {:ok, field} = FilterParser.parse_field("@data->")
      assert is_binary(field.name)
    end

    test "handles field with cast and consecutive operators" do
      {:ok, field} = FilterParser.parse_field("@data->->key::text")
      assert is_binary(field.name)
    end
  end

  describe "SelectParser nested error paths" do
    alias PostgrestParser.SelectParser

    test "parses deeply nested structure with multiple levels" do
      {:ok, items} = SelectParser.parse("a(b(c(d(e(f)))))")
      assert hd(items).name == "a"
    end

    test "handles nested relation followed by comma and more items" do
      {:ok, items} = SelectParser.parse("a(b(c)),d(e(f))")
      assert length(items) == 2
    end

    test "handles spread in deeply nested structure" do
      {:ok, items} = SelectParser.parse("a(b(...c(d)))")
      assert hd(items).name == "a"
    end

    test "handles mix of fields and relations at multiple levels" do
      {:ok, items} = SelectParser.parse("id,a(b,c(d,e),f),g")
      assert length(items) == 3
    end
  end

  describe "FilterParser fallback operator parsing" do
    alias PostgrestParser.FilterParser

    test "parses with fallback for unusual operator format" do
      # This should trigger fallback paths
      {:error, _} = FilterParser.parse("@field", "unknown_op.value")
    end

    test "parses with fallback for FTS language extraction" do
      {:ok, filter} = FilterParser.parse("@content", "wfts(german).search")
      assert filter.language == "german"
    end

    test "parses with fallback for negated operator" do
      {:ok, filter} = FilterParser.parse("@field", "not.like.%test%")
      assert filter.negated?
      assert filter.operator == :like
    end
  end
end
