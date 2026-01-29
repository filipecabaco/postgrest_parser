defmodule ParserBench do
  @moduledoc """
  Performance benchmarks for PostgrestParser.

  Run with: mix run bench/parser_bench.exs
  """

  alias PostgrestParser.SelectParser.AliasParser

  # Sample queries representing different complexity levels
  @simple_queries [
    "id,name,email",
    "id,name,email,created_at,updated_at",
    "id,name,email,created_at,updated_at,status,role,last_login"
  ]

  @alias_queries [
    "user_id:id,user_name:name",
    "uid:id,uname:name,uemail:email,created:created_at",
    "u_id:id,u_name:name,u_email:email,u_created:created_at,u_updated:updated_at"
  ]

  @cast_queries [
    "price::numeric,created_at::timestamp",
    "amount::numeric,date::timestamp,active::boolean",
    "price::numeric,quantity::integer,total::numeric,date::timestamp"
  ]

  @json_queries [
    "data->name,data->email",
    "profile->address->city,profile->address->country",
    "settings->preferences->theme,settings->preferences->language,metadata->tags"
  ]

  @complex_queries [
    "data->user->name:username,data->user->email::text:user_email",
    "profile->data::jsonb:user_profile,settings->prefs->theme:ui_theme",
    "data->nested->value::text:val,price::numeric:total,created_at::timestamp:created"
  ]

  def run do
    IO.puts("\n🚀 PostgrestParser Benchmark Suite\n")
    IO.puts("=" <> String.duplicate("=", 79))

    # Benchmark 1: Alias Parser - Simple Fields
    IO.puts("\n📊 Alias Parser Benchmarks\n")

    Benchee.run(
      %{
        "simple field" => fn -> AliasParser.parse("name") end,
        "field with underscore" => fn -> AliasParser.parse("user_name") end,
        "simple alias" => fn -> AliasParser.parse("username:name") end,
        "alias with underscore" => fn -> AliasParser.parse("user_name:full_name") end
      },
      title: "Simple Field Parsing",
      time: 2,
      memory_time: 1,
      formatters: [Benchee.Formatters.Console]
    )

    # Benchmark 2: Type Casts
    Benchee.run(
      %{
        "field with cast" => fn -> AliasParser.parse("price::numeric") end,
        "cast with alias" => fn -> AliasParser.parse("price::numeric:total") end,
        "timestamp cast" => fn -> AliasParser.parse("created_at::timestamp") end,
        "complex cast+alias" => fn -> AliasParser.parse("amount::numeric:total_amount") end
      },
      title: "Type Cast Parsing",
      time: 2,
      memory_time: 1,
      formatters: [Benchee.Formatters.Console]
    )

    # Benchmark 3: JSON Paths
    Benchee.run(
      %{
        "simple json path" => fn -> AliasParser.parse("data->name") end,
        "double arrow" => fn -> AliasParser.parse("data->>name") end,
        "nested json path" => fn -> AliasParser.parse("data->user->profile->name") end,
        "json with alias" => fn -> AliasParser.parse("username:data->user->name") end,
        "json with cast" => fn -> AliasParser.parse("data->price::numeric") end,
        "json+cast+alias" => fn -> AliasParser.parse("data->price::numeric:total") end
      },
      title: "JSON Path Parsing",
      time: 2,
      memory_time: 1,
      formatters: [Benchee.Formatters.Console]
    )

    # Benchmark 4: Edge Cases (uses fallback parser)
    Benchee.run(
      %{
        "wildcard" => fn -> AliasParser.parse("*") end,
        "aliased wildcard" => fn -> AliasParser.parse("all:*") end,
        "field with hint" => fn -> AliasParser.parse("author!inner") end,
        "alias+hint" => fn -> AliasParser.parse("display_name:author!inner") end,
        "spread operator" => fn -> AliasParser.parse("...profile") end,
        "empty string" => fn -> AliasParser.parse("") end
      },
      title: "Edge Cases (Fallback Parser)",
      time: 2,
      memory_time: 1,
      formatters: [Benchee.Formatters.Console]
    )

    # Benchmark 5: Full Query Parsing
    IO.puts("\n📊 Full Query Parsing Benchmarks\n")

    Benchee.run(
      %{
        "simple (3 fields)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@simple_queries, 0))
        end,
        "simple (5 fields)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@simple_queries, 1))
        end,
        "simple (8 fields)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@simple_queries, 2))
        end,
        "with aliases (2)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@alias_queries, 0))
        end,
        "with aliases (4)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@alias_queries, 1))
        end,
        "with casts (2)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@cast_queries, 0))
        end,
        "with casts (4)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@cast_queries, 2))
        end,
        "json paths (2)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@json_queries, 0))
        end,
        "json paths (3 nested)" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@json_queries, 1))
        end,
        "complex mixed" => fn ->
          PostgrestParser.parse_query_string(Enum.at(@complex_queries, 1))
        end
      },
      title: "Full Query Parsing",
      time: 2,
      memory_time: 1,
      formatters: [Benchee.Formatters.Console]
    )

    # Benchmark 6: SQL Generation
    IO.puts("\n📊 End-to-End SQL Generation Benchmarks\n")

    Benchee.run(
      %{
        "simple select" => fn ->
          PostgrestParser.query_string_to_sql("users", "select=id,name,email")
        end,
        "with filters" => fn ->
          PostgrestParser.query_string_to_sql(
            "users",
            "select=id,name&active=eq.true&role=eq.admin"
          )
        end,
        "with ordering" => fn ->
          PostgrestParser.query_string_to_sql(
            "users",
            "select=id,name,created_at&order=created_at.desc"
          )
        end,
        "complex query" => fn ->
          PostgrestParser.query_string_to_sql(
            "users",
            "select=id,name:full_name,created_at::timestamp&active=eq.true&order=name.asc&limit=100"
          )
        end,
        "with json paths" => fn ->
          PostgrestParser.query_string_to_sql(
            "users",
            "select=id,data->profile->name:username,settings->theme"
          )
        end
      },
      title: "End-to-End SQL Generation",
      time: 2,
      memory_time: 1,
      formatters: [Benchee.Formatters.Console]
    )

    IO.puts("\n" <> String.duplicate("=", 80))
    IO.puts("✅ Benchmark suite completed!\n")
  end
end

# Run the benchmarks
ParserBench.run()
