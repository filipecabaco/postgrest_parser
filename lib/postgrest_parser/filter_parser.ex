defmodule PostgrestParser.FilterParser do
  @moduledoc """
  Parses PostgREST filter expressions into AST structures.

  Supports the full PostgREST filter syntax including:
  - Comparison operators: eq, neq, gt, gte, lt, lte
  - Pattern matching: like, ilike, match, imatch
  - Set operations: in, cs, cd, ov
  - Full-text search: fts, plfts, phfts, wfts
  - Null checks: is (null, not_null, true, false, unknown)
  - Range operations: sl, sr, nxl, nxr, adj
  - Negation: not.operator
  - JSON path: field->key, field->>key
  """

  alias PostgrestParser.AST.Field
  alias PostgrestParser.AST.Filter

  @comparison_operators ~w(eq neq gt gte lt lte)
  @pattern_operators ~w(like ilike match imatch)
  @set_operators ~w(in cs cd ov)
  @fts_operators ~w(fts plfts phfts wfts)
  @range_operators ~w(sl sr nxl nxr adj)
  @all_operators @comparison_operators ++
                   @pattern_operators ++
                   @set_operators ++ @fts_operators ++ @range_operators ++ ["is"]

  @doc """
  Parses a filter key-value pair into a Filter struct.

  ## Examples

      iex> PostgrestParser.FilterParser.parse("id", "eq.1")
      {:ok, %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, operator: :eq, value: "1", negated?: false}}

      iex> PostgrestParser.FilterParser.parse("name", "not.eq.john")
      {:ok, %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "name", json_path: []}, operator: :eq, value: "john", negated?: true}}

      iex> PostgrestParser.FilterParser.parse("status", "in.(active,pending)")
      {:ok, %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "status", json_path: []}, operator: :in, value: ["active", "pending"], negated?: false}}

      iex> PostgrestParser.FilterParser.parse("deleted_at", "is.null")
      {:ok, %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "deleted_at", json_path: []}, operator: :is, value: "null", negated?: false}}

      iex> PostgrestParser.FilterParser.parse("data->name", "eq.test")
      {:ok, %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "data", json_path: [{:arrow, "name"}]}, operator: :eq, value: "test", negated?: false}}

      iex> PostgrestParser.FilterParser.parse("id", "invalid.1")
      {:error, "unknown operator: invalid"}
  """
  @spec parse(String.t(), String.t()) :: {:ok, Filter.t()} | {:error, String.t()}
  def parse(field_str, value_str) do
    with {:ok, field} <- parse_field(field_str),
         {:ok, {negated, operator, value}} <- parse_operator_value(value_str),
         {:ok, parsed_value} <- parse_value(operator, value) do
      {:ok, %Filter{field: field, operator: operator, value: parsed_value, negated?: negated}}
    end
  end

  @doc """
  Parses a field name, handling JSON path operators.

  ## Examples

      iex> PostgrestParser.FilterParser.parse_field("id")
      {:ok, %PostgrestParser.AST.Field{name: "id", json_path: []}}

      iex> PostgrestParser.FilterParser.parse_field("data->key")
      {:ok, %PostgrestParser.AST.Field{name: "data", json_path: [{:arrow, "key"}]}}

      iex> PostgrestParser.FilterParser.parse_field("data->>key")
      {:ok, %PostgrestParser.AST.Field{name: "data", json_path: [{:double_arrow, "key"}]}}

      iex> PostgrestParser.FilterParser.parse_field("data->outer->>inner")
      {:ok, %PostgrestParser.AST.Field{name: "data", json_path: [{:arrow, "outer"}, {:double_arrow, "inner"}]}}
  """
  @spec parse_field(String.t()) :: {:ok, Field.t()} | {:error, String.t()}
  def parse_field(field_str) when is_binary(field_str) do
    case parse_json_path(field_str) do
      {:ok, name, json_path} -> {:ok, %Field{name: name, json_path: json_path}}
      {:error, _} = error -> error
    end
  end

  def parse_field(_), do: {:error, "field must be a string"}

  defp parse_json_path(str) do
    case String.split(str, ~r/->>?/, include_captures: true) do
      [name] ->
        {:ok, name, []}

      [name | rest] ->
        case parse_json_segments(rest, []) do
          {:ok, path} -> {:ok, name, path}
          {:error, _} = error -> error
        end
    end
  end

  defp parse_json_segments([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_json_segments(["->>" | [key | rest]], acc) do
    parse_json_segments(rest, [{:double_arrow, key} | acc])
  end

  defp parse_json_segments(["->" | [key | rest]], acc) do
    parse_json_segments(rest, [{:arrow, key} | acc])
  end

  defp parse_json_segments(_, _), do: {:error, "invalid JSON path syntax"}

  defp parse_operator_value(value_str) do
    case String.split(value_str, ".", parts: 2) do
      ["not", rest] ->
        case parse_operator_value(rest) do
          {:ok, {_negated, operator, value}} -> {:ok, {true, operator, value}}
          {:error, _} = error -> error
        end

      [operator, value] when operator in @all_operators ->
        {:ok, {false, String.to_atom(operator), value}}

      [operator, _value] ->
        {:error, "unknown operator: #{operator}"}

      [_] ->
        {:error, "missing operator or value"}
    end
  end

  defp parse_value(:in, value) do
    case extract_list(value) do
      {:ok, items} -> {:ok, items}
      {:error, _} = error -> error
    end
  end

  defp parse_value(:cs, value), do: {:ok, value}
  defp parse_value(:cd, value), do: {:ok, value}
  defp parse_value(:ov, value), do: extract_list(value)
  defp parse_value(_operator, value), do: {:ok, value}

  defp extract_list(value) do
    case Regex.run(~r/^\((.*)\)$/, value) do
      [_, inner] ->
        items = split_list_items(inner)
        {:ok, items}

      nil ->
        {:error, "expected list format: (item1,item2,...)"}
    end
  end

  defp split_list_items(str) do
    str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.map(&unquote_value/1)
  end

  defp unquote_value("\"" <> rest) do
    rest
    |> String.trim_trailing("\"")
    |> String.replace("\\\"", "\"")
  end

  defp unquote_value(value), do: value

  @doc """
  Checks if a key is a reserved PostgREST parameter (not a filter).
  """
  @spec reserved_key?(String.t()) :: boolean()
  def reserved_key?(key) when key in ~w(select order limit offset on_conflict columns), do: true
  def reserved_key?(_), do: false
end
