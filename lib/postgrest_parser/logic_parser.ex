defmodule PostgrestParser.LogicParser do
  @moduledoc """
  Parses PostgREST boolean logic expressions into AST structures.

  Supports:
  - and(filter1,filter2,...) - logical AND
  - or(filter1,filter2,...) - logical OR
  - not.and(...), not.or(...) - negated logic
  - Nested: and(filter1,or(filter2,filter3))
  """

  alias PostgrestParser.AST.LogicTree
  alias PostgrestParser.FilterParser

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
  def logic_key?("and"), do: true
  def logic_key?("or"), do: true
  def logic_key?("not.and"), do: true
  def logic_key?("not.or"), do: true
  def logic_key?(_), do: false

  defp parse_key("not.and"), do: {true, :and}
  defp parse_key("not.or"), do: {true, :or}
  defp parse_key("and"), do: {false, :and}
  defp parse_key("or"), do: {false, :or}

  defp extract_conditions_str(value) do
    case Regex.run(~r/^\((.*)\)$/, value, capture: :all_but_first) do
      [inner] -> {:ok, inner}
      nil -> {:error, "logic expression must be wrapped in parentheses"}
    end
  end

  defp parse_conditions(str) do
    case split_conditions(str) do
      {:ok, parts} ->
        parts
        |> Enum.reduce_while({:ok, []}, fn part, {:ok, acc} ->
          case parse_condition(String.trim(part)) do
            {:ok, condition} -> {:cont, {:ok, [condition | acc]}}
            {:error, _} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, conditions} -> {:ok, Enum.reverse(conditions)}
          {:error, _} = error -> error
        end

      {:error, _} = error ->
        error
    end
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

  defp parse_condition(condition_str) do
    cond do
      String.starts_with?(condition_str, "and(") or String.starts_with?(condition_str, "or(") ->
        parse_nested_logic(condition_str)

      String.starts_with?(condition_str, "not.and(") or
          String.starts_with?(condition_str, "not.or(") ->
        parse_nested_logic(condition_str)

      true ->
        parse_filter_condition(condition_str)
    end
  end

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
