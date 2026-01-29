defmodule PostgrestParser.LogicParser do
  @moduledoc """
  Parses PostgREST boolean logic expressions into AST structures using NimbleParsec.

  Supports:
  - and(filter1,filter2,...) - logical AND
  - or(filter1,filter2,...) - logical OR
  - not.and(...), not.or(...) - negated logic
  - Nested: and(filter1,or(filter2,filter3))
  """

  import NimbleParsec

  alias PostgrestParser.AST.LogicTree
  alias PostgrestParser.FilterParser

  # Logic operators
  logic_op =
    choice([
      string("and") |> replace(:and),
      string("or") |> replace(:or)
    ])

  # Negation prefix for logic keys
  logic_negation = string("not.") |> replace(true)

  # Logic key parser: "and", "or", "not.and", "not.or"
  logic_key_parser =
    optional(logic_negation |> unwrap_and_tag(:negated))
    |> concat(logic_op |> unwrap_and_tag(:operator))

  defparsec(:parse_key_internal, logic_key_parser)

  # Character that's not a delimiter in conditions
  condition_char = utf8_char(not: ?(, not: ?), not: ?,)

  # Simple text (field.op.value or field=op.value)
  condition_text =
    times(condition_char, min: 1)
    |> reduce({List, :to_string, []})

  # Recursive definitions for nested logic
  defcombinatorp(
    :condition_content,
    parsec(:logic_condition)
    |> repeat(
      ignore(string(","))
      |> concat(parsec(:logic_condition))
    )
  )

  defcombinatorp(
    :nested_logic_expr,
    optional(logic_negation |> unwrap_and_tag(:negated))
    |> concat(logic_op |> unwrap_and_tag(:operator))
    |> ignore(string("("))
    |> concat(parsec(:condition_content) |> tag(:conditions))
    |> ignore(string(")"))
    |> tag(:nested_logic)
  )

  defcombinatorp(
    :simple_condition,
    condition_text
    |> tag(:filter_text)
  )

  # A logic condition is either a nested logic expression or a simple filter
  defcombinatorp(
    :logic_condition,
    choice([
      parsec(:nested_logic_expr),
      parsec(:simple_condition)
    ])
  )

  # Value parser: (conditions...)
  value_parser =
    ignore(string("("))
    |> concat(parsec(:condition_content))
    |> ignore(string(")"))

  defparsec(:parse_value_internal, value_parser)

  @doc """
  Parses a logic expression into a LogicTree struct.

  ## Examples

      iex> PostgrestParser.LogicParser.parse("and", "(id.eq.1,name.eq.john)")
      {:ok, %PostgrestParser.AST.LogicTree{
        operator: :and,
        negated?: false,
        conditions: [
          %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, operator: :eq, value: "1", negated?: false},
          %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "name", json_path: []}, operator: :eq, value: "john", negated?: false}
        ]
      }}

      iex> PostgrestParser.LogicParser.parse("or", "(id.eq.1,id.eq.2)")
      {:ok, %PostgrestParser.AST.LogicTree{
        operator: :or,
        negated?: false,
        conditions: [
          %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, operator: :eq, value: "1", negated?: false},
          %PostgrestParser.AST.Filter{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, operator: :eq, value: "2", negated?: false}
        ]
      }}
  """
  @spec parse(String.t(), String.t()) :: {:ok, LogicTree.t()} | {:error, String.t()}
  def parse(key, value) do
    {negated, operator} = parse_key(key)

    with {:ok, conditions_str} <- extract_conditions_str(value),
         {:ok, conditions} <- parse_conditions(conditions_str) do
      {:ok, %LogicTree{operator: operator, conditions: conditions, negated?: negated}}
    end
  end

  @doc """
  Checks if a key represents a logic operator.

  ## Examples

      iex> PostgrestParser.LogicParser.logic_key?("and")
      true

      iex> PostgrestParser.LogicParser.logic_key?("or")
      true

      iex> PostgrestParser.LogicParser.logic_key?("not.and")
      true

      iex> PostgrestParser.LogicParser.logic_key?("id")
      false
  """
  @spec logic_key?(String.t()) :: boolean()
  def logic_key?(key) do
    case parse_key_internal(key) do
      {:ok, _result, "", _, _, _} -> true
      _ -> false
    end
  end

  defp parse_key(key) do
    case parse_key_internal(key) do
      {:ok, result, "", _, _, _} ->
        negated = Keyword.get(result, :negated, false)
        operator = Keyword.fetch!(result, :operator)
        {negated, operator}

      _ ->
        # Fallback
        fallback_parse_key(key)
    end
  end

  defp fallback_parse_key("not.and"), do: {true, :and}
  defp fallback_parse_key("not.or"), do: {true, :or}
  defp fallback_parse_key("and"), do: {false, :and}
  defp fallback_parse_key("or"), do: {false, :or}

  defp extract_conditions_str(value) do
    case Regex.run(~r/^\((.*)\)$/, value, capture: :all_but_first) do
      [inner] -> {:ok, inner}
      nil -> {:error, "logic expression must be wrapped in parentheses"}
    end
  end

  defp parse_conditions(str) do
    with {:ok, parts} <- split_conditions(str) do
      parse_condition_parts(parts)
    end
  end

  defp parse_condition_parts(parts) do
    parts
    |> Enum.reduce_while({:ok, []}, fn part, {:ok, acc} ->
      case parse_condition(String.trim(part)) do
        {:ok, condition} -> {:cont, {:ok, [condition | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, conditions} -> {:ok, Enum.reverse(conditions)}
      {:error, _} = error -> error
    end)
  end

  defp split_conditions(str) do
    split_at_top_level_commas(str, 0, "", [])
  end

  defp split_at_top_level_commas("", 0, current, acc) do
    {:ok, Enum.reverse([current | acc])}
  end

  defp split_at_top_level_commas("", depth, _current, _acc) when depth > 0 do
    {:error, "unclosed parenthesis in logic expression"}
  end

  defp split_at_top_level_commas("(" <> rest, depth, current, acc) do
    split_at_top_level_commas(rest, depth + 1, current <> "(", acc)
  end

  defp split_at_top_level_commas(")" <> rest, depth, current, acc) when depth > 0 do
    split_at_top_level_commas(rest, depth - 1, current <> ")", acc)
  end

  defp split_at_top_level_commas(")" <> _rest, 0, _current, _acc) do
    {:error, "unexpected closing parenthesis"}
  end

  defp split_at_top_level_commas("," <> rest, 0, current, acc) do
    split_at_top_level_commas(rest, 0, "", [current | acc])
  end

  defp split_at_top_level_commas(<<char::utf8, rest::binary>>, depth, current, acc) do
    split_at_top_level_commas(rest, depth, current <> <<char::utf8>>, acc)
  end

  defp parse_condition("and(" <> _ = condition_str), do: parse_nested_logic(condition_str)
  defp parse_condition("or(" <> _ = condition_str), do: parse_nested_logic(condition_str)
  defp parse_condition("not.and(" <> _ = condition_str), do: parse_nested_logic(condition_str)
  defp parse_condition("not.or(" <> _ = condition_str), do: parse_nested_logic(condition_str)
  defp parse_condition(condition_str), do: parse_filter_condition(condition_str)

  defp parse_nested_logic(str) do
    case Regex.run(~r/^(not\.)?(and|or)\((.+)\)$/, str) do
      [_, negation, operator, inner] ->
        negated = negation == "not."

        with {:ok, conditions} <- parse_conditions(inner) do
          {:ok,
           %LogicTree{
             operator: String.to_atom(operator),
             conditions: conditions,
             negated?: negated
           }}
        end

      nil ->
        {:error, "invalid nested logic: #{str}"}
    end
  end

  defp parse_filter_condition(str) do
    has_equals = String.contains?(str, "=")
    is_logic_equals = String.match?(str, ~r/^(not\.)?(and|or)=/)

    case {has_equals, is_logic_equals} do
      {true, false} -> parse_equals_notation(str)
      _ -> parse_dot_notation(str)
    end
  end

  defp parse_equals_notation(str) do
    case String.split(str, "=", parts: 2) do
      [field, operator_value] -> FilterParser.parse(field, operator_value)
      _ -> parse_dot_notation(str)
    end
  end

  defp parse_dot_notation(str) do
    case String.split(str, ".", parts: 3) do
      [field, "not", rest] ->
        case String.split(rest, ".", parts: 2) do
          [operator, value] ->
            FilterParser.parse(field, "not.#{operator}.#{value}")

          _ ->
            {:error, "invalid filter: #{str}"}
        end

      [field, operator, value] ->
        FilterParser.parse(field, "#{operator}.#{value}")

      _ ->
        {:error, "invalid filter format: #{str}"}
    end
  end
end
