defmodule PostgrestParser.FilterParser do
  @moduledoc """
  Parses PostgREST filter expressions into AST structures using NimbleParsec.

  Supports the full PostgREST filter syntax including:
  - Comparison operators: eq, neq, gt, gte, lt, lte
  - Pattern matching: like, ilike, match, imatch
  - Set operations: in, cs, cd, ov
  - Full-text search: fts, plfts, phfts, wfts
  - Null checks: is (null, not_null, true, false, unknown)
  - Range operations: sl, sr, nxl, nxr, adj
  - Negation: not.operator
  - JSON path: field->key, field->>key
  - Quantifiers: operator(any), operator(all)
  """

  import NimbleParsec

  alias PostgrestParser.AST.Field
  alias PostgrestParser.AST.Filter

  # Operator categories for validation
  @comparison_operators ~w(eq neq gt gte lt lte)a
  @pattern_operators ~w(like ilike match imatch)a
  @set_operators ~w(in cs cd ov)a
  @fts_operators ~w(fts plfts phfts wfts)a
  @range_operators ~w(sl sr nxl nxr adj)a
  @quantifiable_operators ~w(eq neq gt gte lt lte like ilike match imatch)a
  @all_operators @comparison_operators ++
                   @pattern_operators ++
                   @set_operators ++ @fts_operators ++ @range_operators ++ [:is]

  # Basic identifier character
  identifier_char = utf8_char([?a..?z, ?A..?Z, ?0..?9, ?_])

  # Identifier: alphanumeric and underscore
  identifier =
    times(identifier_char, min: 1)
    |> reduce({List, :to_string, []})

  # JSON operators: must try ->> before ->
  json_double_arrow = string("->>") |> replace(:double_arrow)
  json_single_arrow = string("->") |> replace(:arrow)

  json_operator = choice([json_double_arrow, json_single_arrow])

  # JSON path segment: operator + identifier
  json_path_segment =
    json_operator
    |> concat(identifier)
    |> reduce(:build_json_segment)

  # Field with optional JSON path and type cast
  field_parser =
    identifier
    |> unwrap_and_tag(:name)
    |> concat(repeat(json_path_segment) |> tag(:json_path))
    |> concat(
      optional(
        ignore(string("::"))
        |> concat(identifier |> unwrap_and_tag(:cast))
      )
    )

  defparsec(:parse_field_internal, field_parser)

  # Comparison operators (order matters: neq before eq, gte before gt, lte before lt)
  comparison_ops =
    choice([
      string("neq") |> replace(:neq),
      string("eq") |> replace(:eq),
      string("gte") |> replace(:gte),
      string("gt") |> replace(:gt),
      string("lte") |> replace(:lte),
      string("lt") |> replace(:lt)
    ])

  # Pattern matching operators (order matters: ilike before like, imatch before match)
  pattern_ops =
    choice([
      string("ilike") |> replace(:ilike),
      string("imatch") |> replace(:imatch),
      string("like") |> replace(:like),
      string("match") |> replace(:match)
    ])

  # Set operators
  set_ops =
    choice([
      string("in") |> replace(:in),
      string("cs") |> replace(:cs),
      string("cd") |> replace(:cd),
      string("ov") |> replace(:ov)
    ])

  # FTS operators (order matters: plfts/phfts/wfts before fts)
  fts_ops =
    choice([
      string("plfts") |> replace(:plfts),
      string("phfts") |> replace(:phfts),
      string("wfts") |> replace(:wfts),
      string("fts") |> replace(:fts)
    ])

  # Range operators
  range_ops =
    choice([
      string("nxl") |> replace(:nxl),
      string("nxr") |> replace(:nxr),
      string("adj") |> replace(:adj),
      string("sl") |> replace(:sl),
      string("sr") |> replace(:sr)
    ])

  # Is operator
  is_op = string("is") |> replace(:is)

  # Quantifier: (any) or (all)
  quantifier =
    ignore(string("("))
    |> choice([
      string("any") |> replace(:any),
      string("all") |> replace(:all)
    ])
    |> ignore(string(")"))

  # FTS language: (english), (spanish), etc.
  fts_language =
    ignore(string("("))
    |> concat(identifier)
    |> ignore(string(")"))

  # Quantifiable operators with optional quantifier
  quantifiable_with_quantifier =
    choice([comparison_ops, pattern_ops])
    |> unwrap_and_tag(:operator)
    |> concat(optional(quantifier |> unwrap_and_tag(:quantifier)))

  # FTS operators with optional language
  fts_with_language =
    fts_ops
    |> unwrap_and_tag(:operator)
    |> concat(optional(fts_language |> unwrap_and_tag(:language)))

  # Other operators (no modifiers)
  other_ops =
    choice([set_ops, range_ops, is_op])
    |> unwrap_and_tag(:operator)

  # Complete operator with optional modifiers
  operator_with_modifier =
    choice([
      quantifiable_with_quantifier,
      fts_with_language,
      other_ops
    ])

  # Negation prefix
  negation = string("not.") |> replace(true)

  # Complete operator-value parser: [not.]operator[modifier].value
  operator_value_parser =
    optional(negation |> unwrap_and_tag(:negated))
    |> concat(operator_with_modifier)
    |> ignore(string("."))
    |> concat(utf8_string([], min: 0) |> unwrap_and_tag(:value))

  defparsec(:parse_operator_value_internal, operator_value_parser)

  # List with parentheses: (item1,item2,...)
  # We need to handle nested content carefully, but for simple lists this works
  list_item_char = utf8_char(not: ?,, not: ?))

  list_item =
    times(list_item_char, min: 0)
    |> reduce({List, :to_string, []})

  paren_list =
    ignore(string("("))
    |> concat(list_item)
    |> repeat(
      ignore(string(","))
      |> concat(list_item)
    )
    |> ignore(string(")"))

  defparsec(:parse_paren_list, paren_list)

  # List with braces: {item1,item2,...}
  brace_item_char = utf8_char(not: ?,, not: ?})

  brace_item =
    times(brace_item_char, min: 0)
    |> reduce({List, :to_string, []})

  brace_list =
    ignore(string("{"))
    |> concat(brace_item)
    |> repeat(
      ignore(string(","))
      |> concat(brace_item)
    )
    |> ignore(string("}"))

  defparsec(:parse_brace_list, brace_list)

  @doc false
  def build_json_segment([op, key]), do: {op, key}

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
         {:ok, {negated, operator, quantifier, language, value}} <-
           parse_operator_value(value_str),
         {:ok, parsed_value} <- parse_value(operator, quantifier, value) do
      {:ok,
       %Filter{
         field: field,
         operator: operator,
         quantifier: quantifier,
         language: language,
         value: parsed_value,
         negated?: negated
       }}
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
    case parse_field_internal(field_str) do
      {:ok, result, "", _, _, _} ->
        {:ok, extract_field(result)}

      {:ok, _result, _rest, _, _, _} ->
        # Fallback for edge cases
        fallback_parse_field(field_str)

      {:error, _reason, _rest, _, _, _} ->
        fallback_parse_field(field_str)
    end
  end

  def parse_field(_), do: {:error, "field must be a string"}

  defp extract_field(parsed) do
    name = Keyword.fetch!(parsed, :name)
    json_path = Keyword.get(parsed, :json_path, [])
    cast = Keyword.get(parsed, :cast)

    %Field{name: name, json_path: json_path, cast: cast}
  end

  # Fallback field parser using regex (for edge cases)
  defp fallback_parse_field(field_str) do
    case String.split(field_str, "::", parts: 2) do
      [field_part, cast] ->
        case parse_json_path(field_part) do
          {:ok, name, json_path} -> {:ok, %Field{name: name, json_path: json_path, cast: cast}}
          {:error, _} = error -> error
        end

      [field_part] ->
        case parse_json_path(field_part) do
          {:ok, name, json_path} -> {:ok, %Field{name: name, json_path: json_path}}
          {:error, _} = error -> error
        end
    end
  end

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
    case parse_operator_value_internal(value_str) do
      {:ok, result, "", _, _, _} ->
        extract_operator_value(result)

      {:ok, _result, _rest, _, _, _} ->
        # Fallback for edge cases
        fallback_parse_operator_value(value_str)

      {:error, _reason, _rest, _, _, _} ->
        fallback_parse_operator_value(value_str)
    end
  end

  defp extract_operator_value(parsed) do
    negated = Keyword.get(parsed, :negated, false)
    operator = Keyword.fetch!(parsed, :operator)
    quantifier = Keyword.get(parsed, :quantifier)
    language = Keyword.get(parsed, :language)
    value = Keyword.get(parsed, :value, "")

    case validate_operator_quantifier(operator, quantifier, language, value) do
      {:ok, {_, op, quant, lang, val}} -> {:ok, {negated, op, quant, lang, val}}
      {:error, _} = error -> error
    end
  end

  # Fallback operator-value parser using string manipulation
  defp fallback_parse_operator_value(value_str) do
    case String.split(value_str, ".", parts: 2) do
      ["not", rest] ->
        case fallback_parse_operator_value(rest) do
          {:ok, {_negated, operator, quantifier, language, value}} ->
            {:ok, {true, operator, quantifier, language, value}}

          {:error, _} = error ->
            error
        end

      [operator_str, value] ->
        {:ok, operator, quantifier, language} = extract_quantifier(operator_str)
        validate_operator_quantifier(operator, quantifier, language, value)

      [_] ->
        {:error, "missing operator or value"}
    end
  end

  defp extract_quantifier(operator_str) do
    case Regex.run(~r/^(\w+)\((any|all)\)$/, operator_str) do
      [_, op, quant] ->
        {:ok, String.to_atom(op), String.to_atom(quant), nil}

      nil ->
        extract_fts_language_or_plain(operator_str)
    end
  end

  defp extract_fts_language_or_plain(operator_str) do
    case Regex.run(~r/^(fts|plfts|phfts|wfts)\((\w+)\)$/, operator_str) do
      [_, op, lang] -> {:ok, String.to_atom(op), nil, lang}
      nil -> {:ok, String.to_atom(operator_str), nil, nil}
    end
  end

  defp validate_operator_quantifier(operator, quantifier, _language, value)
       when operator in @quantifiable_operators and quantifier in [:any, :all] do
    {:ok, {false, operator, quantifier, nil, value}}
  end

  defp validate_operator_quantifier(operator, nil, language, _value)
       when operator in @fts_operators and language in ["any", "all"] do
    {:error, "operator #{operator} does not support quantifiers"}
  end

  defp validate_operator_quantifier(operator, nil, language, value)
       when operator in @fts_operators and is_binary(language) do
    {:ok, {false, operator, nil, language, value}}
  end

  defp validate_operator_quantifier(operator, nil, nil, value)
       when operator in @all_operators do
    {:ok, {false, operator, nil, nil, value}}
  end

  defp validate_operator_quantifier(operator, quantifier, _language, _value)
       when not is_nil(quantifier) do
    {:error, "operator #{operator} does not support quantifiers"}
  end

  defp validate_operator_quantifier(operator, _, _, _) do
    {:error, "unknown operator: #{operator}"}
  end

  defp parse_value(operator, quantifier, value)
       when operator in @quantifiable_operators and quantifier in [:any, :all] do
    case parse_brace_list(value) do
      {:ok, items, "", _, _, _} ->
        {:ok, Enum.map(items, &unquote_and_trim/1)}

      _ ->
        # Fallback
        case Regex.run(~r/^\{(.*)\}$/, value) do
          [_, inner] ->
            items = split_list_items(inner)
            {:ok, items}

          nil ->
            {:error, "quantified operators require list format: {item1,item2}"}
        end
    end
  end

  defp parse_value(:in, nil, value) do
    case extract_list(value) do
      {:ok, items} -> {:ok, items}
      {:error, _} = error -> error
    end
  end

  defp parse_value(:cs, nil, value), do: {:ok, value}
  defp parse_value(:cd, nil, value), do: {:ok, value}
  defp parse_value(:ov, nil, value), do: extract_list(value)
  defp parse_value(_operator, nil, value), do: {:ok, value}

  defp extract_list(value) do
    case parse_paren_list(value) do
      {:ok, items, "", _, _, _} ->
        {:ok, Enum.map(items, &unquote_and_trim/1)}

      _ ->
        # Fallback
        case Regex.run(~r/^\((.*)\)$/, value) do
          [_, inner] ->
            items = split_list_items(inner)
            {:ok, items}

          nil ->
            {:error, "expected list format: (item1,item2,...)"}
        end
    end
  end

  defp split_list_items(str) do
    str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.map(&unquote_value/1)
  end

  defp unquote_and_trim(str) do
    str |> String.trim() |> unquote_value()
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
