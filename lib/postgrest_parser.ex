defmodule PostgrestParser do
  @moduledoc """
  PostgREST URL parameter parser and SQL query builder.

  This module provides a complete implementation of PostgREST's URL-to-SQL
  parsing system, allowing filtering, selecting, ordering, and pagination
  of PostgreSQL data using URL query parameters.

  ## Usage

      # Parse a query string into AST
      {:ok, params} = PostgrestParser.parse_query_string("select=id,name&id=eq.1&order=id.desc")

      # Build SQL from parsed params
      {:ok, query} = PostgrestParser.to_sql("users", params)
      # => %{query: ~s(SELECT "id", "name" FROM "users" WHERE "id" = $1 ORDER BY "id" DESC), params: [1]}

  ## Supported Features

  - **Filters**: eq, neq, gt, gte, lt, lte, like, ilike, in, is, fts, etc.
  - **Negation**: not.eq, not.in, etc.
  - **Logic trees**: and(filter1,filter2), or(filter1,filter2)
  - **Select**: column selection with aliases, relations, spreads
  - **Order**: ordering with direction and nulls handling
  - **Pagination**: limit and offset
  - **JSON paths**: data->key, data->>key
  """

  alias PostgrestParser.AST.ParsedParams
  alias PostgrestParser.FilterParser
  alias PostgrestParser.LogicParser
  alias PostgrestParser.OrderParser
  alias PostgrestParser.SelectParser
  alias PostgrestParser.SqlBuilder

  @doc """
  Parses a URL query string into a ParsedParams struct.

  ## Examples

      iex> PostgrestParser.parse_query_string("id=eq.1")
      {:ok, %PostgrestParser.AST.ParsedParams{
        select: nil,
        filters: [%PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, operator: :eq, value: "1", negated?: false}],
        order: [],
        limit: nil,
        offset: nil
      }}

      iex> PostgrestParser.parse_query_string("select=id,name&order=id.desc&limit=10")
      {:ok, %PostgrestParser.AST.ParsedParams{
        select: [
          %PostgrestParser.AST.SelectItem{type: :field, name: "id", alias: nil, children: nil, hint: nil},
          %PostgrestParser.AST.SelectItem{type: :field, name: "name", alias: nil, children: nil, hint: nil}
        ],
        filters: [],
        order: [%PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :desc, nulls: nil}],
        limit: 10,
        offset: nil
      }}

      iex> PostgrestParser.parse_query_string("")
      {:ok, %PostgrestParser.AST.ParsedParams{select: nil, filters: [], order: [], limit: nil, offset: nil}}
  """
  @spec parse_query_string(String.t()) :: {:ok, ParsedParams.t()} | {:error, String.t()}
  def parse_query_string(query_string) when is_binary(query_string) do
    query_string
    |> URI.decode_query()
    |> parse_params()
  end

  @doc """
  Parses a map of query parameters into a ParsedParams struct.

  ## Examples

      iex> PostgrestParser.parse_params(%{"id" => "eq.1", "select" => "*"})
      {:ok, %PostgrestParser.AST.ParsedParams{
        select: [%PostgrestParser.AST.SelectItem{type: :field, name: "*", alias: nil, children: nil, hint: nil}],
        filters: [%PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, operator: :eq, value: "1", negated?: false}],
        order: [],
        limit: nil,
        offset: nil
      }}
  """
  @spec parse_params(map()) :: {:ok, ParsedParams.t()} | {:error, String.t()}
  def parse_params(params) when is_map(params) do
    with {:ok, select} <- parse_select(params),
         {:ok, filters} <- parse_filters(params),
         {:ok, order} <- parse_order(params),
         {:ok, limit} <- parse_limit(params),
         {:ok, offset} <- parse_offset(params) do
      {:ok,
       %ParsedParams{select: select, filters: filters, order: order, limit: limit, offset: offset}}
    end
  end

  @doc """
  Converts parsed parameters into a SQL query for the given table.

  ## Examples

      iex> {:ok, params} = PostgrestParser.parse_query_string("select=*&id=eq.1")
      iex> PostgrestParser.to_sql("users", params)
      {:ok, %{query: ~s(SELECT * FROM "users" WHERE "id" = $1), params: [1]}}
  """
  @spec to_sql(String.t(), ParsedParams.t()) :: {:ok, map()} | {:error, String.t()}
  def to_sql(table, %ParsedParams{} = params) when is_binary(table) do
    SqlBuilder.build_select(table, params)
  end

  @doc """
  Converts parsed parameters into a SQL query with embedded relations.

  Uses schema cache to resolve relationships and generate LATERAL JOINs
  for related data. This enables PostgREST-style resource embedding.

  ## Parameters

    - `tenant_id` - Tenant identifier for schema cache lookup
    - `schema` - Database schema name (e.g., "public")
    - `table` - Main table name
    - `params` - Parsed parameters from parse_query_string/1
  """
  @spec to_sql_with_relations(String.t(), String.t(), String.t(), ParsedParams.t()) ::
          {:ok, map()} | {:error, String.t()}
  def to_sql_with_relations(tenant_id, schema, table, %ParsedParams{} = params)
      when is_binary(tenant_id) and is_binary(schema) and is_binary(table) do
    SqlBuilder.build_select_with_relations(tenant_id, schema, table, params)
  end

  @doc """
  Parses a query string and directly generates SQL with embedded relations.

  Convenience function combining parse_query_string/1 and to_sql_with_relations/4.
  """
  @spec query_string_to_sql_with_relations(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, map()} | {:error, String.t()}
  def query_string_to_sql_with_relations(tenant_id, schema, table, query_string) do
    with {:ok, params} <- parse_query_string(query_string) do
      to_sql_with_relations(tenant_id, schema, table, params)
    end
  end

  @doc """
  Parses a query string and directly generates SQL.

  Convenience function combining parse_query_string/1 and to_sql/2.

  ## Examples

      iex> PostgrestParser.query_string_to_sql("users", "select=*&id=eq.1")
      {:ok, %{query: ~s(SELECT * FROM "users" WHERE "id" = $1), params: [1]}}
  """
  @spec query_string_to_sql(String.t(), String.t()) :: {:ok, map()} | {:error, String.t()}
  def query_string_to_sql(table, query_string) do
    with {:ok, params} <- parse_query_string(query_string) do
      to_sql(table, params)
    end
  end

  @doc """
  Builds only the WHERE clause from filter parameters.

  Useful for CDC subscriptions where you only need the filter part.

  ## Examples

      iex> PostgrestParser.build_filter_clause(%{"id" => "eq.1", "status" => "in.(active,pending)"})
      {:ok, %{clause: ~s["id" = $1 AND "status" = ANY($2)], params: [1, ["active", "pending"]]}}
  """
  @spec build_filter_clause(map()) :: {:ok, map()} | {:error, String.t()}
  def build_filter_clause(params) when is_map(params) do
    with {:ok, filters} <- parse_filters(params) do
      SqlBuilder.build_where_clause(filters)
    end
  end

  defp parse_select(params) do
    case Map.get(params, "select") do
      nil -> {:ok, nil}
      select_str -> SelectParser.parse(select_str)
    end
  end

  defp parse_filters(params) do
    params
    |> Enum.reject(fn {key, _} -> FilterParser.reserved_key?(key) end)
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, acc} ->
      result =
        if LogicParser.logic_key?(key) do
          LogicParser.parse(key, value)
        else
          FilterParser.parse(key, value)
        end

      case result do
        {:ok, filter} -> {:cont, {:ok, [filter | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, filters} -> {:ok, Enum.reverse(filters)}
      {:error, _} = error -> error
    end
  end

  defp parse_order(params) do
    case Map.get(params, "order") do
      nil -> {:ok, []}
      order_str -> OrderParser.parse(order_str)
    end
  end

  defp parse_limit(params) do
    case Map.get(params, "limit") do
      nil -> {:ok, nil}
      limit_str -> parse_integer(limit_str, "limit")
    end
  end

  defp parse_offset(params) do
    case Map.get(params, "offset") do
      nil -> {:ok, nil}
      offset_str -> parse_integer(offset_str, "offset")
    end
  end

  defp parse_integer(str, name) do
    case Integer.parse(str) do
      {int, ""} when int >= 0 -> {:ok, int}
      _ -> {:error, "#{name} must be a non-negative integer"}
    end
  end
end
