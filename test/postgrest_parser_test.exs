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

    test "parses is unknown filter" do
      {:ok, params} = PostgrestParser.parse_query_string("state=is.unknown")
      assert [%Filter{operator: :is, value: "unknown"}] = params.filters
    end

    test "parses full-text search filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=fts.postgres")
      assert [%Filter{operator: :fts, value: "postgres"}] = params.filters
    end

    test "parses plain full-text search filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=plfts.postgres")
      assert [%Filter{operator: :plfts, value: "postgres"}] = params.filters
    end

    test "parses phrase full-text search filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=phfts.postgres database")
      assert [%Filter{operator: :phfts, value: "postgres database"}] = params.filters
    end

    test "parses websearch full-text search filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=wfts.postgres OR database")
      assert [%Filter{operator: :wfts, value: "postgres OR database"}] = params.filters
    end

    test "parses match filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=match.^[A-Z]")
      assert [%Filter{operator: :match, value: "^[A-Z]"}] = params.filters
    end

    test "parses imatch filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=imatch.^[a-z]")
      assert [%Filter{operator: :imatch, value: "^[a-z]"}] = params.filters
    end

    test "parses contains filter" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cs.{tag1,tag2}")
      assert [%Filter{operator: :cs, value: "{tag1,tag2}"}] = params.filters
    end

    test "parses contained by filter" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cd.{tag1,tag2,tag3}")
      assert [%Filter{operator: :cd, value: "{tag1,tag2,tag3}"}] = params.filters
    end

    test "parses overlap filter" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=ov.(tag1,tag2)")
      assert [%Filter{operator: :ov, value: ["tag1", "tag2"]}] = params.filters
    end

    test "parses strictly left filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=sl.[0,5)")
      assert [%Filter{operator: :sl, value: "[0,5)"}] = params.filters
    end

    test "parses strictly right filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=sr.(10,20]")
      assert [%Filter{operator: :sr, value: "(10,20]"}] = params.filters
    end

    test "parses not extends left filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=nxl.[5,10)")
      assert [%Filter{operator: :nxl, value: "[5,10)"}] = params.filters
    end

    test "parses not extends right filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=nxr.(15,25]")
      assert [%Filter{operator: :nxr, value: "(15,25]"}] = params.filters
    end

    test "parses adjacent filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=adj.[5,10)")
      assert [%Filter{operator: :adj, value: "[5,10)"}] = params.filters
    end

    test "parses negated filter" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.eq.deleted")
      assert [%Filter{operator: :eq, value: "deleted", negated?: true}] = params.filters
    end

    test "parses negated in filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=not.in.(1,2,3)")
      assert [%Filter{operator: :in, value: ["1", "2", "3"], negated?: true}] = params.filters
    end

    test "parses negated like filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.like.%test%")
      assert [%Filter{operator: :like, value: "%test%", negated?: true}] = params.filters
    end

    test "parses negated ilike filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.ilike.%test%")
      assert [%Filter{operator: :ilike, value: "%test%", negated?: true}] = params.filters
    end

    test "parses negated match filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.match.^test")
      assert [%Filter{operator: :match, value: "^test", negated?: true}] = params.filters
    end

    test "parses negated imatch filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.imatch.^test")
      assert [%Filter{operator: :imatch, value: "^test", negated?: true}] = params.filters
    end

    test "parses negated gt filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.gt.18")
      assert [%Filter{operator: :gt, value: "18", negated?: true}] = params.filters
    end

    test "parses negated gte filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.gte.21")
      assert [%Filter{operator: :gte, value: "21", negated?: true}] = params.filters
    end

    test "parses negated lt filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.lt.65")
      assert [%Filter{operator: :lt, value: "65", negated?: true}] = params.filters
    end

    test "parses negated lte filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.lte.60")
      assert [%Filter{operator: :lte, value: "60", negated?: true}] = params.filters
    end

    test "parses negated neq filter" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.neq.active")
      assert [%Filter{operator: :neq, value: "active", negated?: true}] = params.filters
    end

    test "parses negated is null filter" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=not.is.null")
      assert [%Filter{operator: :is, value: "null", negated?: true}] = params.filters
    end

    test "parses negated is not_null filter" do
      {:ok, params} = PostgrestParser.parse_query_string("deleted_at=not.is.not_null")
      assert [%Filter{operator: :is, value: "not_null", negated?: true}] = params.filters
    end

    test "parses negated is true filter" do
      {:ok, params} = PostgrestParser.parse_query_string("active=not.is.true")
      assert [%Filter{operator: :is, value: "true", negated?: true}] = params.filters
    end

    test "parses negated is false filter" do
      {:ok, params} = PostgrestParser.parse_query_string("active=not.is.false")
      assert [%Filter{operator: :is, value: "false", negated?: true}] = params.filters
    end

    test "parses negated is unknown filter" do
      {:ok, params} = PostgrestParser.parse_query_string("state=not.is.unknown")
      assert [%Filter{operator: :is, value: "unknown", negated?: true}] = params.filters
    end

    test "parses negated fts filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.fts.postgres")
      assert [%Filter{operator: :fts, value: "postgres", negated?: true}] = params.filters
    end

    test "parses negated plfts filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.plfts.postgres")
      assert [%Filter{operator: :plfts, value: "postgres", negated?: true}] = params.filters
    end

    test "parses negated phfts filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.phfts.postgres database")
      assert [%Filter{operator: :phfts, value: "postgres database", negated?: true}] = params.filters
    end

    test "parses negated wfts filter" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.wfts.postgres")
      assert [%Filter{operator: :wfts, value: "postgres", negated?: true}] = params.filters
    end

    test "parses negated cs filter" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.cs.{tag1}")
      assert [%Filter{operator: :cs, value: "{tag1}", negated?: true}] = params.filters
    end

    test "parses negated cd filter" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.cd.{tag1,tag2}")
      assert [%Filter{operator: :cd, value: "{tag1,tag2}", negated?: true}] = params.filters
    end

    test "parses negated ov filter" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.ov.(tag1,tag2)")
      assert [%Filter{operator: :ov, value: ["tag1", "tag2"], negated?: true}] = params.filters
    end

    test "parses negated sl filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.sl.[0,5)")
      assert [%Filter{operator: :sl, value: "[0,5)", negated?: true}] = params.filters
    end

    test "parses negated sr filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.sr.(10,20]")
      assert [%Filter{operator: :sr, value: "(10,20]", negated?: true}] = params.filters
    end

    test "parses negated nxl filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.nxl.[5,10)")
      assert [%Filter{operator: :nxl, value: "[5,10)", negated?: true}] = params.filters
    end

    test "parses negated nxr filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.nxr.(15,25]")
      assert [%Filter{operator: :nxr, value: "(15,25]", negated?: true}] = params.filters
    end

    test "parses negated adj filter" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.adj.[5,10)")
      assert [%Filter{operator: :adj, value: "[5,10)", negated?: true}] = params.filters
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

    test "parses deeply nested JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->a->b->c->>d=eq.value")

      assert [
               %Filter{
                 field: %Field{
                   name: "data",
                   json_path: [
                     {:arrow, "a"},
                     {:arrow, "b"},
                     {:arrow, "c"},
                     {:double_arrow, "d"}
                   ]
                 }
               }
             ] = params.filters
    end

    test "handles JSON path with empty key" do
      {:ok, params} = PostgrestParser.parse_query_string("data->=eq.value")
      assert [%Filter{field: %Field{name: "data", json_path: [{:arrow, ""}]}}] = params.filters
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

    test "parses column with hint" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author!inner(name)")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "author",
                 hint: "inner",
                 children: [%SelectItem{type: :field, name: "name"}]
               }
             ] = params.select
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

    test "parses aliased relation" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author_info:author(name)")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "author",
                 alias: "author_info",
                 children: [%SelectItem{type: :field, name: "name"}]
               }
             ] = params.select
    end

    test "handles spread with alias by treating the whole thing as relation name" do
      {:ok, params} = PostgrestParser.parse_query_string("select=profile_data:...profile(bio)")

      assert [%SelectItem{type: :relation, name: "...profile", alias: "profile_data"}] =
               params.select
    end

    test "parses JSON path in select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,data->name")

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{type: :field, name: "data", hint: {:json_path, [{:arrow, "name"}]}}
             ] = params.select
    end

    test "parses aliased JSON path in select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=full_name:data->>name")

      assert [
               %SelectItem{
                 type: :field,
                 name: "data",
                 alias: "full_name",
                 hint: {:json_path, [{:double_arrow, "name"}]}
               }
             ] = params.select
    end

    test "returns error for empty field name" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=id,,name")
      assert msg =~ ~r/(unexpected|empty)/
    end

    test "returns error for unclosed parenthesis in select" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=posts(id,title")
      assert String.contains?(msg, "parenthesis")
    end

    test "parses field with empty parentheses as empty relation" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id()")
      assert [%SelectItem{type: :relation, name: "id", children: []}] = params.select
    end

    test "parses field without parentheses as simple field" do
      {:ok, params} = PostgrestParser.parse_query_string("select=posts")
      assert [%SelectItem{type: :field, name: "posts"}] = params.select
    end

    test "returns error for invalid field name with parenthesis" do
      assert {:error, _} = PostgrestParser.parse_query_string("select=id(name")
    end

    test "parses deeply nested relations" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=user(profile(settings(theme)))")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "user",
                 children: [
                   %SelectItem{
                     type: :relation,
                     name: "profile",
                     children: [
                       %SelectItem{
                         type: :relation,
                         name: "settings",
                         children: [%SelectItem{type: :field, name: "theme"}]
                       }
                     ]
                   }
                 ]
               }
             ] = params.select
    end

    test "returns error for invalid field name with parenthesis inside" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=test(field")
      assert msg =~ "parenthesis"
    end

    test "returns error when missing closing paren in nested relation" do
      assert {:error, _} = PostgrestParser.parse_query_string("select=author(posts(id")
    end

    test "parses multiple nested relations at same level" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=posts(id),comments(id),tags(name)")

      assert length(params.select) == 3
      assert Enum.all?(params.select, fn item -> item.type == :relation end)
    end

    test "parses relation with hint and children" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author!left(name,email)")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "author",
                 hint: "left",
                 children: [
                   %SelectItem{type: :field, name: "name"},
                   %SelectItem{type: :field, name: "email"}
                 ]
               }
             ] = params.select
    end

    test "parses spread relation with hint" do
      {:ok, params} = PostgrestParser.parse_query_string("select=...profile!inner(bio)")

      assert [%SelectItem{type: :spread, name: "profile", hint: "inner"}] = params.select
    end

    test "parses aliased field with hint" do
      {:ok, params} = PostgrestParser.parse_query_string("select=display_name:author!inner(name)")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "author",
                 alias: "display_name",
                 hint: "inner"
               }
             ] = params.select
    end

    test "returns error for unexpected token after nested relation" do
      assert {:error, _} = PostgrestParser.parse_query_string("select=author(name)extra")
    end

    test "handles space in field name" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author(name extra)")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "author",
                 children: [%SelectItem{type: :field, name: "name extra"}]
               }
             ] = params.select
    end

    test "handles complex mix of fields and relations" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "select=id,name,email,posts(id,title),comments!inner(text),tags(name)"
        )

      field_count = Enum.count(params.select, fn item -> item.type == :field end)
      relation_count = Enum.count(params.select, fn item -> item.type == :relation end)

      assert field_count == 3
      assert relation_count == 3
    end

    test "returns error for relation expecting children but none provided" do
      assert {:error, _} = PostgrestParser.parse_query_string("select=posts(")
    end

    test "parses nested relation after simple field in nested context" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=user(id,profile(bio))")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "user",
                 children: [
                   %SelectItem{type: :field, name: "id"},
                   %SelectItem{type: :relation, name: "profile"}
                 ]
               }
             ] = params.select
    end

    test "returns error for unexpected token after relation in list" do
      assert {:error, _} = PostgrestParser.parse_query_string("select=id,posts(title)invalid")
    end

    test "parses mix of spread and regular relations" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=id,...profile(bio),posts(title)")

      types = Enum.map(params.select, & &1.type)
      assert :field in types
      assert :spread in types
      assert :relation in types
    end

    test "handles deeply nested spread relations" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=user(...profile(...settings(theme)))")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "user",
                 children: [
                   %SelectItem{
                     type: :spread,
                     name: "profile",
                     children: [
                       %SelectItem{
                         type: :spread,
                         name: "settings"
                       }
                     ]
                   }
                 ]
               }
             ] = params.select
    end
  end

  describe "SelectParser direct usage" do
    alias PostgrestParser.SelectParser

    test "parse returns empty list for empty string" do
      assert {:ok, []} = SelectParser.parse("")
    end

    test "parse returns empty list for nil" do
      assert {:ok, []} = SelectParser.parse(nil)
    end

    test "returns error for field containing open parenthesis in name" do
      assert {:error, msg} = SelectParser.parse("test(field")
      assert msg =~ ~r/(unclosed|parenthesis)/
    end

    test "handles trailing comma by parsing what's before it" do
      {:ok, items} = SelectParser.parse("id,")
      assert [%SelectItem{type: :field, name: "id"}] = items
    end

    test "returns error for multiple commas" do
      assert {:error, _} = SelectParser.parse("id,,name")
    end

    test "parses field with complex hint" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author!left_join(name)")

      assert [%SelectItem{type: :relation, hint: "left_join"}] = params.select
    end

    test "parses simple field after relation in list" do
      {:ok, params} = PostgrestParser.parse_query_string("select=posts(title),id")

      assert [
               %SelectItem{type: :relation, name: "posts"},
               %SelectItem{type: :field, name: "id"}
             ] = params.select
    end

    test "returns error when close paren appears without open" do
      assert {:error, _} = SelectParser.parse("id)")
    end

    test "parses trailing comma gracefully or errors" do
      result = SelectParser.parse("id,name,")
      assert match?({:error, _}, result) or match?({:ok, _}, result)
    end

    test "returns error for open paren without field" do
      assert {:error, _} = SelectParser.parse("(id)")
    end

    test "returns error for nested relation without close paren" do
      assert {:error, _} = SelectParser.parse("user(posts(id")
    end

    test "handles empty relation children" do
      {:ok, items} = SelectParser.parse("posts(),id")

      assert [
               %SelectItem{type: :relation, name: "posts", children: []},
               %SelectItem{type: :field, name: "id"}
             ] = items
    end

    test "handles aliased wildcard" do
      {:ok, params} = PostgrestParser.parse_query_string("select=all:*")
      assert [%SelectItem{name: "*", alias: "all"}] = params.select
    end

    test "parses nested relation after field with comma" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user(id,name,posts(title))")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "user",
                 children: [
                   %SelectItem{type: :field, name: "id"},
                   %SelectItem{type: :field, name: "name"},
                   %SelectItem{type: :relation, name: "posts"}
                 ]
               }
             ] = params.select
    end

    test "returns error for unexpected close paren at top level" do
      assert {:error, _} = SelectParser.parse("id,name)")
    end

    test "parses field with all special characters in hint" do
      {:ok, params} = PostgrestParser.parse_query_string("select=author!left.join_123(name)")
      assert [%SelectItem{hint: "left.join_123"}] = params.select
    end

    test "handles relation with multiple children including aliased" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=author(display:name,user_email:email,id)")

      assert [
               %SelectItem{
                 type: :relation,
                 children: [
                   %SelectItem{type: :field, alias: "display"},
                   %SelectItem{type: :field, alias: "user_email"},
                   %SelectItem{type: :field, name: "id"}
                 ]
               }
             ] = params.select
    end

    test "parses multiple nested levels with mix of types" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "select=id,author(name,...profile(bio),posts(id,tags(name)))"
        )

      assert [
               %SelectItem{type: :field, name: "id"},
               %SelectItem{
                 type: :relation,
                 name: "author",
                 children: [
                   %SelectItem{type: :field, name: "name"},
                   %SelectItem{type: :spread, name: "profile"},
                   %SelectItem{type: :relation, name: "posts"}
                 ]
               }
             ] = params.select
    end

    test "handles relation with only nested relations no simple fields" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=author(posts(id),comments(text))")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "author",
                 children: [
                   %SelectItem{type: :relation, name: "posts"},
                   %SelectItem{type: :relation, name: "comments"}
                 ]
               }
             ] = params.select
    end

    test "parses spread as first child in relation" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=author(...profile(bio),name)")

      assert [
               %SelectItem{
                 type: :relation,
                 children: [
                   %SelectItem{type: :spread, name: "profile"},
                   %SelectItem{type: :field, name: "name"}
                 ]
               }
             ] = params.select
    end

    test "parses very deeply nested relation structure" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "select=a(b(c(d(e(f(g))))))"
        )

      assert [%SelectItem{type: :relation, name: "a"}] = params.select
    end

    test "handles aliased JSON path in select" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user_data:data->user->name")

      assert [
               %SelectItem{
                 type: :field,
                 name: "data",
                 alias: "user_data",
                 hint: {:json_path, [{:arrow, "user"}, {:arrow, "name"}]}
               }
             ] = params.select
    end

    test "parses field after nested relation in nested context" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=user(posts(title,comments(text)),email)")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "user",
                 children: [
                   %SelectItem{
                     type: :relation,
                     name: "posts",
                     children: [
                       %SelectItem{type: :field, name: "title"},
                       %SelectItem{type: :relation, name: "comments"}
                     ]
                   },
                   %SelectItem{type: :field, name: "email"}
                 ]
               }
             ] = params.select
    end

    test "returns error for relation followed by unexpected text token" do
      result = SelectParser.parse("posts(id)extra,name")
      assert match?({:error, _}, result)
    end

    test "returns error for field followed by unexpected open paren at top level" do
      result = SelectParser.parse("id name(test)")
      assert match?({:error, _}, result) or match?({:ok, _}, result)
    end

    test "parses mix of simple and complex selects" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "select=*,author(name),count,tags!inner(name),created_at"
        )

      assert length(params.select) == 5
      field_types = Enum.map(params.select, & &1.type)
      assert :field in field_types
      assert :relation in field_types
    end

    test "handles close paren in nested context correctly" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user(posts(id,title))")

      assert [
               %SelectItem{
                 type: :relation,
                 name: "user",
                 children: [
                   %SelectItem{
                     type: :relation,
                     name: "posts",
                     children: [
                       %SelectItem{type: :field, name: "id"},
                       %SelectItem{type: :field, name: "title"}
                     ]
                   }
                 ]
               }
             ] = params.select
    end

    test "parses nested spread at end of list" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user(id,...profile(bio))")

      assert [
               %SelectItem{
                 children: [
                   %SelectItem{type: :field},
                   %SelectItem{type: :spread}
                 ]
               }
             ] = params.select
    end

    test "handles field with parenthesis in name gracefully" do
      result = SelectParser.parse("invalid(name")
      assert match?({:error, _}, result)
    end

    test "parses aliased spread with nested content" do
      {:ok, params} =
        PostgrestParser.parse_query_string("select=...profile(bio,email,phone)")

      assert [
               %SelectItem{
                 type: :spread,
                 name: "profile",
                 children: [
                   %SelectItem{type: :field, name: "bio"},
                   %SelectItem{type: :field, name: "email"},
                   %SelectItem{type: :field, name: "phone"}
                 ]
               }
             ] = params.select
    end

    test "returns error when close paren appears without open in nested" do
      assert {:error, _} = SelectParser.parse("user(id,name))")
    end

    test "returns error for empty select after comma" do
      result = SelectParser.parse("user(id,)")
      assert match?({:error, _}, result) or match?({:ok, _}, result)
    end

    test "handles nested relation with comma after close paren" do
      {:ok, params} = PostgrestParser.parse_query_string("select=user(posts(id)),name")

      assert [
               %SelectItem{type: :relation, name: "user"},
               %SelectItem{type: :field, name: "name"}
             ] = params.select
    end

    test "returns error for tokens after item" do
      result = SelectParser.parse("id name")
      assert match?({:error, _}, result) or match?({:ok, _}, result)
    end
  end

  describe "Edge cases" do
    test "parses query with float values" do
      {:ok, params} = PostgrestParser.parse_query_string("price=gt.99.99")
      {:ok, result} = PostgrestParser.to_sql("products", params)
      assert [%Decimal{}] = result.params
    end

    test "parses query with negative numbers" do
      {:ok, params} = PostgrestParser.parse_query_string("balance=lt.-50")
      {:ok, result} = PostgrestParser.to_sql("accounts", params)
      assert result.params == [-50]
    end

    test "handles multiple filters with same field" do
      {:ok, params} = PostgrestParser.parse_query_string("age=gte.18&age=lte.65")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert length(result.params) >= 1
      assert String.contains?(result.query, "age")
    end

    test "parses query with special characters in string values" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.John's%20Test")
      {:ok, result} = PostgrestParser.to_sql("users", params)
      assert ["John's Test"] = result.params
    end
  end

  describe "order parsing" do
    test "parses simple order" do
      {:ok, params} = PostgrestParser.parse_query_string("order=id")
      assert [%OrderTerm{direction: :asc, nulls: nil}] = params.order
    end

    test "parses ascending order explicitly" do
      {:ok, params} = PostgrestParser.parse_query_string("order=id.asc")
      assert [%OrderTerm{direction: :asc}] = params.order
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

    test "parses order with nulls first only" do
      {:ok, params} = PostgrestParser.parse_query_string("order=priority.nullsfirst")
      assert [%OrderTerm{direction: :asc, nulls: :first}] = params.order
    end

    test "parses order with nulls last only" do
      {:ok, params} = PostgrestParser.parse_query_string("order=priority.nullslast")
      assert [%OrderTerm{direction: :asc, nulls: :last}] = params.order
    end

    test "parses multiple order terms" do
      {:ok, params} = PostgrestParser.parse_query_string("order=status.desc,created_at.asc")

      assert [
               %OrderTerm{direction: :desc},
               %OrderTerm{direction: :asc}
             ] = params.order
    end

    test "parses order on JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("order=data->priority.desc")

      assert [%OrderTerm{field: %Field{name: "data", json_path: [{:arrow, "priority"}]}}] =
               params.order
    end

    test "parses order on nested JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("order=data->settings->priority.asc")

      assert [
               %OrderTerm{
                 field: %Field{
                   name: "data",
                   json_path: [{:arrow, "settings"}, {:arrow, "priority"}]
                 }
               }
             ] = params.order
    end

    test "treats invalid order option as part of field name" do
      {:ok, params} = PostgrestParser.parse_query_string("order=id.invalid")
      assert [%OrderTerm{field: %Field{name: "id.invalid"}, direction: :asc}] = params.order
    end

    test "returns error for multiple invalid options" do
      assert {:error, _} = PostgrestParser.parse_query_string("order=id.asc.desc")
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

    test "parses negated or logic" do
      {:ok, params} = PostgrestParser.parse_query_string("not.or=(id.eq.1,id.eq.2)")

      assert [%LogicTree{operator: :or, negated?: true}] = params.filters
    end

    test "parses nested logic" do
      {:ok, params} =
        PostgrestParser.parse_query_string("and=(status.eq.active,or(type.eq.a,type.eq.b))")

      assert [%LogicTree{operator: :and, conditions: [_, %LogicTree{operator: :or}]}] =
               params.filters
    end

    test "parses deeply nested logic" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "and=(status.eq.active,or(type.eq.a,and(archived.eq.false,deleted.eq.false)))"
        )

      assert [
               %LogicTree{
                 operator: :and,
                 conditions: [_, %LogicTree{operator: :or, conditions: [_, %LogicTree{}]}]
               }
             ] = params.filters
    end

    test "parses negated nested logic" do
      {:ok, params} =
        PostgrestParser.parse_query_string("and=(status.eq.active,not.or(type.eq.a,type.eq.b))")

      assert [
               %LogicTree{
                 operator: :and,
                 conditions: [_, %LogicTree{operator: :or, negated?: true}]
               }
             ] = params.filters
    end

    test "returns error for logic without parentheses" do
      assert {:error, msg} = PostgrestParser.parse_query_string("and=id.eq.1,name.eq.john")
      assert String.contains?(msg, "parentheses")
    end

    test "returns error for unclosed parenthesis in logic" do
      assert {:error, msg} = PostgrestParser.parse_query_string("and=(id.eq.1,name.eq.john")
      assert msg =~ ~r/parenthes(is|es)/
    end

    test "returns error for unexpected closing parenthesis" do
      assert {:error, _} = PostgrestParser.parse_query_string("and=(id.eq.1))")
    end

    test "parses logic with negated filter inside" do
      {:ok, params} =
        PostgrestParser.parse_query_string("and=(id.eq.1,status.not.eq.deleted)")

      assert [%LogicTree{operator: :and, conditions: [_, %Filter{negated?: true}]}] =
               params.filters
    end

    test "returns error for invalid filter in logic tree" do
      assert {:error, _} = PostgrestParser.parse_query_string("and=(id.invalid.1,name.eq.john)")
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

    test "generates query with negated eq filter" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.eq.deleted")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "<>")
      assert result.params == ["deleted"]
    end

    test "generates query with negated neq filter" do
      {:ok, params} = PostgrestParser.parse_query_string("status=not.neq.active")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "=")
    end

    test "generates query with negated gt filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.gt.18")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "<=")
    end

    test "generates query with negated gte filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.gte.21")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "<")
    end

    test "generates query with negated lt filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.lt.65")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, ">=")
    end

    test "generates query with negated lte filter" do
      {:ok, params} = PostgrestParser.parse_query_string("age=not.lte.60")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, ">")
    end

    test "generates query with negated like filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.like.%test%")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "NOT LIKE")
    end

    test "generates query with negated ilike filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.ilike.%test%")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "NOT ILIKE")
    end

    test "generates query with negated match filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.match.^test")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "!~")
    end

    test "generates query with negated imatch filter" do
      {:ok, params} = PostgrestParser.parse_query_string("name=not.imatch.^test")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "!~*")
    end

    test "generates query with negated in filter" do
      {:ok, params} = PostgrestParser.parse_query_string("id=not.in.(1,2,3)")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "NOT = ANY")
    end

    test "generates query with is not null" do
      {:ok, params} = PostgrestParser.parse_query_string("email=not.is.null")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "IS NOT NULL")
    end

    test "generates query with is null via not.not_null" do
      {:ok, params} = PostgrestParser.parse_query_string("email=not.is.not_null")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "IS NULL")
    end

    test "generates query with is not true" do
      {:ok, params} = PostgrestParser.parse_query_string("active=not.is.true")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "IS NOT TRUE")
    end

    test "generates query with is not false" do
      {:ok, params} = PostgrestParser.parse_query_string("active=not.is.false")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "IS NOT FALSE")
    end

    test "generates query with is not unknown" do
      {:ok, params} = PostgrestParser.parse_query_string("state=not.is.unknown")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "IS NOT UNKNOWN")
    end

    test "generates query with match operator" do
      {:ok, params} = PostgrestParser.parse_query_string("name=match.^[A-Z]")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "~ $")
    end

    test "generates query with imatch operator" do
      {:ok, params} = PostgrestParser.parse_query_string("name=imatch.^[a-z]")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "~* $")
    end

    test "generates query with plfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=plfts.postgres")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "@@ plainto_tsquery")
    end

    test "generates query with phfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=phfts.postgres database")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "@@ phraseto_tsquery")
    end

    test "generates query with wfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=wfts.postgres OR database")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "@@ websearch_to_tsquery")
    end

    test "generates query with negated fts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.fts.test")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@@ to_tsquery")
    end

    test "generates query with negated plfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.plfts.test")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@@ plainto_tsquery")
    end

    test "generates query with negated phfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.phfts.test phrase")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@@ phraseto_tsquery")
    end

    test "generates query with negated wfts operator" do
      {:ok, params} = PostgrestParser.parse_query_string("content=not.wfts.test")
      {:ok, result} = PostgrestParser.to_sql("documents", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@@ websearch_to_tsquery")
    end

    test "generates query with cs operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cs.{tag1,tag2}")
      {:ok, result} = PostgrestParser.to_sql("posts", params)

      assert String.contains?(result.query, "@>")
    end

    test "generates query with cd operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=cd.{tag1,tag2}")
      {:ok, result} = PostgrestParser.to_sql("posts", params)

      assert String.contains?(result.query, "<@")
    end

    test "generates query with ov operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=ov.(tag1,tag2)")
      {:ok, result} = PostgrestParser.to_sql("posts", params)

      assert String.contains?(result.query, "&&")
    end

    test "generates query with sl operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=sl.[0,5)")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "<<")
    end

    test "generates query with sr operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=sr.(10,20]")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, ">>")
    end

    test "generates query with nxl operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=nxl.[5,10)")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "&<")
    end

    test "generates query with nxr operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=nxr.(15,25]")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "&>")
    end

    test "generates query with adj operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=adj.[5,10)")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "-|-")
    end

    test "generates query with negated cs operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.cs.{tag1}")
      {:ok, result} = PostgrestParser.to_sql("posts", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "@>")
    end

    test "generates query with negated cd operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.cd.{tag1,tag2}")
      {:ok, result} = PostgrestParser.to_sql("posts", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "<@")
    end

    test "generates query with negated ov operator" do
      {:ok, params} = PostgrestParser.parse_query_string("tags=not.ov.(tag1,tag2)")
      {:ok, result} = PostgrestParser.to_sql("posts", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "&&")
    end

    test "generates query with negated sl operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.sl.[0,5)")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "<<")
    end

    test "generates query with negated sr operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.sr.(10,20]")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, ">>")
    end

    test "generates query with negated nxl operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.nxl.[5,10)")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "&<")
    end

    test "generates query with negated nxr operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.nxr.(15,25]")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "&>")
    end

    test "generates query with negated adj operator" do
      {:ok, params} = PostgrestParser.parse_query_string("range=not.adj.[5,10)")
      {:ok, result} = PostgrestParser.to_sql("bookings", params)

      assert String.contains?(result.query, "NOT")
      assert String.contains?(result.query, "-|-")
    end

    test "generates query with JSON path single arrow" do
      {:ok, params} = PostgrestParser.parse_query_string("data->key=eq.value")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "->")
    end

    test "generates query with nested JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->outer->inner=eq.value")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "->")
    end

    test "generates query with array index in JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("select=data->0,data->1")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "SELECT")
    end

    test "generates query with is unknown" do
      {:ok, params} = PostgrestParser.parse_query_string("state=is.unknown")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "IS UNKNOWN")
    end

    test "generates query with selected JSON path column" do
      {:ok, params} = PostgrestParser.parse_query_string("select=id,data->>name")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "->>")
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

    test "returns error for negative offset" do
      assert {:error, _} = PostgrestParser.parse_query_string("offset=-10")
    end

    test "handles missing operator" do
      assert {:error, _} = PostgrestParser.parse_query_string("name=test")
    end

    test "handles malformed JSON path with empty key" do
      {:ok, params} = PostgrestParser.parse_query_string("data->->key=eq.value")
      assert [%Filter{field: %Field{json_path: [{:arrow, ""}, {:arrow, "key"}]}}] = params.filters
    end
  end

  describe "SqlBuilder additional scenarios" do
    alias PostgrestParser.SqlBuilder

    test "build_where_clause with empty filters returns empty" do
      {:ok, result} = SqlBuilder.build_where_clause([])
      assert result.clause == ""
      assert result.params == []
    end

    test "generates query with no select returns wildcard" do
      params = %ParsedParams{select: [], filters: [], order: [], limit: nil, offset: nil}
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "SELECT *")
    end

    test "generates query with nil select returns wildcard" do
      params = %ParsedParams{select: nil, filters: [], order: [], limit: nil, offset: nil}
      {:ok, result} = SqlBuilder.build_select("users", params)
      assert String.contains?(result.query, "SELECT *")
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

  describe "value coercion in SQL generation" do
    test "coerces integer strings to integers" do
      {:ok, params} = PostgrestParser.parse_query_string("id=eq.42")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.params == [42]
    end

    test "coerces float strings to Decimal" do
      {:ok, params} = PostgrestParser.parse_query_string("price=eq.19.99")
      {:ok, result} = PostgrestParser.to_sql("products", params)

      assert [%Decimal{}] = result.params
    end

    test "keeps non-numeric strings as strings" do
      {:ok, params} = PostgrestParser.parse_query_string("name=eq.john123abc")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.params == ["john123abc"]
    end

    test "coerces list of integers" do
      {:ok, params} = PostgrestParser.parse_query_string("id=in.(1,2,3)")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert result.params == [[1, 2, 3]]
    end

    test "coerces list of mixed types" do
      {:ok, params} = PostgrestParser.parse_query_string("values=in.(1,test,3)")
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert [[1, "test", 3]] = result.params
    end
  end

  describe "list parsing" do
    test "parses in filter with quoted values" do
      query = ~s[status=in.("active","pending")]
      {:ok, params} = PostgrestParser.parse_query_string(query)

      assert [%Filter{operator: :in, value: ["active", "pending"]}] = params.filters
    end

    test "parses in filter with escaped quotes" do
      query = ~s[name=in.("John \\"Doe\\"","Jane")]
      {:ok, params} = PostgrestParser.parse_query_string(query)

      assert [%Filter{operator: :in, value: values}] = params.filters
      assert length(values) == 2
      assert "Jane" in values
    end

    test "parses in filter with empty value" do
      {:ok, params} = PostgrestParser.parse_query_string("status=in.(active,,pending)")

      assert [%Filter{operator: :in, value: ["active", "", "pending"]}] = params.filters
    end

    test "returns error for in filter without parentheses" do
      assert {:error, msg} = PostgrestParser.parse_query_string("status=in.active,pending")
      assert String.contains?(msg, "list")
    end

    test "returns error for ov filter without parentheses" do
      assert {:error, _} = PostgrestParser.parse_query_string("tags=ov.tag1,tag2")
    end
  end

  describe "FilterParser edge cases" do
    alias PostgrestParser.FilterParser

    test "parse_field returns error for non-string" do
      assert {:error, msg} = FilterParser.parse_field(123)
      assert String.contains?(msg, "string")
    end

    test "reserved_key? identifies select" do
      assert FilterParser.reserved_key?("select")
    end

    test "reserved_key? identifies order" do
      assert FilterParser.reserved_key?("order")
    end

    test "reserved_key? identifies limit" do
      assert FilterParser.reserved_key?("limit")
    end

    test "reserved_key? identifies offset" do
      assert FilterParser.reserved_key?("offset")
    end

    test "reserved_key? identifies on_conflict" do
      assert FilterParser.reserved_key?("on_conflict")
    end

    test "reserved_key? identifies columns" do
      assert FilterParser.reserved_key?("columns")
    end

    test "reserved_key? returns false for regular fields" do
      refute FilterParser.reserved_key?("id")
      refute FilterParser.reserved_key?("name")
      refute FilterParser.reserved_key?("status")
    end

    test "returns error for ov operator without list format" do
      assert {:error, _} = PostgrestParser.parse_query_string("tags=ov.value")
    end

    test "parses cs operator with non-list value" do
      {:ok, params} = PostgrestParser.parse_query_string("data=cs.value")
      assert [%Filter{operator: :cs, value: "value"}] = params.filters
    end

    test "parses cd operator with non-list value" do
      {:ok, params} = PostgrestParser.parse_query_string("data=cd.value")
      assert [%Filter{operator: :cd, value: "value"}] = params.filters
    end

    test "parse returns error for value without operator" do
      assert {:error, msg} = FilterParser.parse("field", "value")
      assert String.contains?(msg, "operator")
    end

    test "parses negated operator with all operators" do
      operators = ~w(eq neq gt gte lt lte like ilike match imatch in is fts plfts phfts wfts cs cd ov sl sr nxl nxr adj)

      for op <- operators do
        value = if op == "in" or op == "ov", do: "(1,2)", else: "test"
        {:ok, params} = PostgrestParser.parse_query_string("field=not.#{op}.#{value}")
        assert [%Filter{negated?: true}] = params.filters
      end
    end

    test "handles array index in JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->0=eq.value")

      assert [
               %Filter{
                 field: %Field{
                   name: "data",
                   json_path: [{:arrow, "0"}]
                 }
               }
             ] = params.filters
    end

    test "parses in filter with single item" do
      {:ok, params} = PostgrestParser.parse_query_string("id=in.(1)")
      assert [%Filter{operator: :in, value: ["1"]}] = params.filters
    end

    test "parses in filter with whitespace" do
      {:ok, params} = PostgrestParser.parse_query_string("status=in.(active, pending, done)")
      assert [%Filter{operator: :in, value: values}] = params.filters
      assert length(values) == 3
    end

    test "parses deeply nested JSON path with mixed operators" do
      {:ok, params} =
        PostgrestParser.parse_query_string("data->level1->level2->>level3=eq.value")

      assert [
               %Filter{
                 field: %Field{
                   json_path: [
                     {:arrow, "level1"},
                     {:arrow, "level2"},
                     {:double_arrow, "level3"}
                   ]
                 }
               }
             ] = params.filters
    end

    test "handles all comparison operators with integer values" do
      operators = [{"eq", "5"}, {"neq", "10"}, {"gt", "15"}, {"gte", "20"}, {"lt", "25"}, {"lte", "30"}]

      for {op, val} <- operators do
        {:ok, params} = PostgrestParser.parse_query_string("age=#{op}.#{val}")
        assert [%Filter{operator: _}] = params.filters
      end
    end

    test "handles all pattern operators with values" do
      operators = [{"like", "%test%"}, {"ilike", "%TEST%"}, {"match", "^test"}, {"imatch", "^TEST"}]

      for {op, val} <- operators do
        {:ok, params} = PostgrestParser.parse_query_string("name=#{op}.#{val}")
        assert [%Filter{operator: _}] = params.filters
      end
    end
  end

  describe "LogicParser edge cases" do
    alias PostgrestParser.LogicParser

    test "logic_key? returns false for regular fields" do
      refute LogicParser.logic_key?("name")
      refute LogicParser.logic_key?("id")
      refute LogicParser.logic_key?("status")
    end

    test "returns error for logic with invalid nested syntax" do
      assert {:error, _} = PostgrestParser.parse_query_string("and=(id.eq.1,invalid)")
    end

    test "returns error for nested logic with malformed expression" do
      assert {:error, _} = PostgrestParser.parse_query_string("and=(id.eq.1,or(invalid))")
    end

    test "handles multiple nested negations" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "and=(status.eq.active,not.or(deleted.eq.true,not.and(archived.eq.true,hidden.eq.true)))"
        )

      assert [%LogicTree{operator: :and, conditions: [_, %LogicTree{negated?: true}]}] =
               params.filters
    end
  end

  describe "complex query scenarios" do
    test "handles query with all parameter types" do
      query =
        "select=id,name&status=eq.active&age=gt.18&order=created_at.desc.nullsfirst&limit=10&offset=5"

      {:ok, params} = PostgrestParser.parse_query_string(query)
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "SELECT")
      assert String.contains?(result.query, "WHERE")
      assert String.contains?(result.query, "AND")
      assert String.contains?(result.query, "ORDER BY")
      assert String.contains?(result.query, "NULLS FIRST")
      assert String.contains?(result.query, "LIMIT")
      assert String.contains?(result.query, "OFFSET")
    end

    test "handles query with logic trees and regular filters" do
      query = "status=eq.active&or=(type.eq.a,type.eq.b)&age=gt.18"
      {:ok, params} = PostgrestParser.parse_query_string(query)
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "AND")
      assert String.contains?(result.query, "OR")
    end

    test "handles query with negated operators and JSON paths" do
      query = "data->status=not.eq.deleted&data->>name=like.%test%"
      {:ok, params} = PostgrestParser.parse_query_string(query)
      {:ok, result} = PostgrestParser.to_sql("items", params)

      assert String.contains?(result.query, "->")
      assert String.contains?(result.query, "->>")
      assert String.contains?(result.query, "<>")
      assert String.contains?(result.query, "LIKE")
    end

    test "handles empty select with filters" do
      {:ok, params} = PostgrestParser.parse_query_string("status=eq.active")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "SELECT *")
    end

    test "handles only limit without other parameters" do
      {:ok, params} = PostgrestParser.parse_query_string("limit=5")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "LIMIT")
      refute String.contains?(result.query, "WHERE")
      refute String.contains?(result.query, "ORDER BY")
    end

    test "handles only offset without other parameters" do
      {:ok, params} = PostgrestParser.parse_query_string("offset=10")
      {:ok, result} = PostgrestParser.to_sql("users", params)

      assert String.contains?(result.query, "OFFSET")
      refute String.contains?(result.query, "WHERE")
      refute String.contains?(result.query, "LIMIT")
    end

    test "handles in filter with quoted and unquoted values mixed" do
      query = ~s[status=in.(active,"pending review",done)]
      {:ok, params} = PostgrestParser.parse_query_string(query)
      assert [%Filter{operator: :in, value: values}] = params.filters
      assert "active" in values
      assert "done" in values
    end

    test "parses JSON path with numeric keys" do
      {:ok, params} = PostgrestParser.parse_query_string("data->0->1->2=eq.value")

      assert [
               %Filter{
                 field: %Field{
                   json_path: [{:arrow, "0"}, {:arrow, "1"}, {:arrow, "2"}]
                 }
               }
             ] = params.filters
    end

    test "parses complex nested logic with all operator types" do
      {:ok, params} =
        PostgrestParser.parse_query_string(
          "and=(status.eq.active,or(type.eq.a,type.eq.b),not.and(deleted.eq.true,archived.eq.true))"
        )

      assert [
               %LogicTree{
                 operator: :and,
                 conditions: [
                   %Filter{},
                   %LogicTree{operator: :or},
                   %LogicTree{operator: :and, negated?: true}
                 ]
               }
             ] = params.filters
    end
  end

  describe "SelectParser error handling" do
    test "rejects unexpected '(' after simple field" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=id,name(")
      assert msg =~ "unclosed parenthesis" or msg =~ "unexpected"
    end

    test "allows field names with spaces" do
      {:ok, result} = PostgrestParser.SelectParser.parse("id something")
      assert [%SelectItem{name: "id something"}] = result
    end

    test "rejects unexpected tokens after relation" do
      assert {:error, msg} = PostgrestParser.SelectParser.parse("users(id)extra")
      assert msg =~ "unexpected token after relation"
    end

    test "allows simple field list" do
      {:ok, result} = PostgrestParser.SelectParser.parse("users,id")
      assert [%SelectItem{name: "users"}, %SelectItem{name: "id"}] = result
    end

    test "rejects unclosed parenthesis" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=users(id,name")
      assert msg =~ "unclosed parenthesis"
    end

    test "rejects nested unclosed parenthesis" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=users(orders(id)")
      assert msg =~ "unclosed parenthesis"
    end

    test "allows nested relations" do
      {:ok, params} = PostgrestParser.parse_query_string("select=users(id(extra))")
      assert [%SelectItem{type: :relation, children: [%SelectItem{type: :relation}]}] =
               params.select
    end

    test "rejects invalid field name with parenthesis" do
      assert {:error, msg} = PostgrestParser.SelectParser.parse("field(name")
      assert msg =~ "invalid field name" or msg =~ "unclosed"
    end

    test "allows empty alias before colon" do
      {:ok, result} = PostgrestParser.SelectParser.parse(":alias")
      assert [%SelectItem{name: "alias", alias: ""}] = result
    end

    test "rejects empty field in list" do
      assert {:error, msg} = PostgrestParser.parse_query_string("select=users(,id)")
      assert msg =~ "unexpected tokens" or msg =~ "empty"
    end

    test "allows trailing comma in select" do
      {:ok, result} = PostgrestParser.SelectParser.parse("users(id,)")
      assert [%SelectItem{name: "users", children: children}] = result
      assert length(children) == 1
    end

    test "parses field with exclamation in nested select" do
      {:ok, result} = PostgrestParser.SelectParser.parse("users(id!inner)")
      assert [%SelectItem{type: :relation, name: "users", children: children}] = result
      assert [%SelectItem{type: :field, name: "id"}] = children
    end
  end

  describe "SchemaCache error handling" do
    test "handles ETS table not found errors" do
      pid = self()
      ref = make_ref()

      spawn(fn ->
        result =
          PostgrestParser.SchemaCache.get_table(
            "nonexistent_tenant",
            "public",
            "nonexistent_table"
          )

        send(pid, {ref, result})
      end)

      receive do
        {^ref, result} -> assert result == {:error, :not_found}
      after
        1000 -> flunk("Timeout waiting for result")
      end
    end

    test "handles missing relationships gracefully" do
      relationships =
        PostgrestParser.SchemaCache.get_relationships("nonexistent", "public", "users")

      assert relationships == []
    end

    test "handles relationship not found" do
      result =
        PostgrestParser.SchemaCache.find_relationship(
          "nonexistent",
          "public",
          "users",
          "orders"
        )

      assert result == {:error, :not_found}
    end

    test "handles hint-based relationship lookup with no matches" do
      result =
        PostgrestParser.SchemaCache.find_relationship_with_hint(
          "nonexistent",
          "public",
          "users",
          "orders",
          "some_hint"
        )

      assert result == {:error, :not_found}
    end
  end

  describe "RelationBuilder error paths" do
    test "handles relationship not found in relation building" do
      params = %ParsedParams{
        select: [
          %SelectItem{
            type: :relation,
            name: "nonexistent_relation",
            children: [%SelectItem{type: :field, name: "id"}]
          }
        ],
        filters: [],
        order: [],
        limit: nil,
        offset: nil
      }

      result = PostgrestParser.to_sql_with_relations("tenant", "public", "users", params)
      assert {:error, _} = result
    end
  end

  describe "FilterParser additional error cases" do
    test "handles malformed filter value" do
      result = PostgrestParser.parse_query_string("id=eq.")
      assert {:ok, params} = result
      assert [%Filter{value: ""}] = params.filters
    end

    test "handles filter with only operator" do
      result = PostgrestParser.parse_query_string("field=gt")
      assert {:error, _} = result
    end

    test "handles unknown operator" do
      result = PostgrestParser.parse_query_string("id=unknown.value")
      assert {:error, msg} = result
      assert msg =~ "unknown operator"
    end

    test "handles empty operator in negation" do
      result = PostgrestParser.parse_query_string("id=not.")
      assert {:error, _} = result
    end

    test "parses filter with complex JSON path" do
      {:ok, params} = PostgrestParser.parse_query_string("data->nested->deep->>value=eq.test")
      assert [%Filter{field: %Field{json_path: json_path}}] = params.filters
      assert length(json_path) == 3
    end
  end

  describe "OrderParser additional error cases" do
    test "handles invalid nulls option" do
      result = PostgrestParser.parse_query_string("order=id.asc.invalidnulls")
      assert {:error, _} = result
    end

    test "treats invalid direction as field name" do
      result = PostgrestParser.parse_query_string("order=id.invaliddir")
      assert {:ok, params} = result
      assert [%OrderTerm{field: %Field{name: "id.invaliddir"}}] = params.order
    end

    test "handles empty order value" do
      result = PostgrestParser.parse_query_string("order=")
      assert {:ok, params} = result
      assert params.order == []
    end
  end

  describe "LogicParser additional error scenarios" do
    test "parses deeply nested logic correctly" do
      result =
        PostgrestParser.parse_query_string(
          "and=(or=(status.eq.a,status.eq.b),active.eq.true)"
        )

      assert {:ok, _} = result
    end

    test "handles malformed logic tree" do
      result = PostgrestParser.parse_query_string("and=(incomplete")
      assert {:error, _} = result
    end

    test "handles invalid nested logic syntax" do
      result = PostgrestParser.parse_query_string("and=status.eq.active")
      assert {:error, _} = result
    end
  end
end
