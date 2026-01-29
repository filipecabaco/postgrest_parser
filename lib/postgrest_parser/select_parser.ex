defmodule PostgrestParser.SelectParser do
  @moduledoc """
  Parses PostgREST select expressions into AST structures.

  Supports:
  - Simple fields: name, id
  - Aliases: alias:name
  - Wildcards: *
  - Relations: client(id,name)
  - Spread relations: ...client(id)
  - Hints: client!inner(id)
  - JSON paths: data->key, data->>key
  """

  alias PostgrestParser.AST.SelectItem
  alias PostgrestParser.FilterParser

  @doc """
  Parses a select string into a list of SelectItem structs.

  ## Examples

      iex> PostgrestParser.SelectParser.parse("*")
      {:ok, [%PostgrestParser.AST.SelectItem{type: :field, name: "*", alias: nil, children: nil, hint: nil}]}

      iex> PostgrestParser.SelectParser.parse("id,name")
      {:ok, [
        %PostgrestParser.AST.SelectItem{type: :field, name: "id", alias: nil, children: nil, hint: nil},
        %PostgrestParser.AST.SelectItem{type: :field, name: "name", alias: nil, children: nil, hint: nil}
      ]}

      iex> PostgrestParser.SelectParser.parse("user_name:name")
      {:ok, [%PostgrestParser.AST.SelectItem{type: :field, name: "name", alias: "user_name", children: nil, hint: nil}]}

      iex> PostgrestParser.SelectParser.parse("client(id,name)")
      {:ok, [%PostgrestParser.AST.SelectItem{type: :relation, name: "client", alias: nil, hint: nil, children: [
        %PostgrestParser.AST.SelectItem{type: :field, name: "id", alias: nil, children: nil, hint: nil},
        %PostgrestParser.AST.SelectItem{type: :field, name: "name", alias: nil, children: nil, hint: nil}
      ]}]}

      iex> PostgrestParser.SelectParser.parse("...client(id)")
      {:ok, [%PostgrestParser.AST.SelectItem{type: :spread, name: "client", alias: nil, hint: nil, children: [
        %PostgrestParser.AST.SelectItem{type: :field, name: "id", alias: nil, children: nil, hint: nil}
      ]}]}
  """
  @spec parse(String.t()) :: {:ok, [SelectItem.t()]} | {:error, String.t()}
  def parse(select_str) when is_binary(select_str) and select_str != "" do
    select_str
    |> tokenize()
    |> parse_items([])
  end

  def parse(""), do: {:ok, []}
  def parse(nil), do: {:ok, []}

  defp tokenize(str) do
    str
    |> String.graphemes()
    |> Enum.reduce({[], ""}, fn char, {tokens, current} ->
      case char do
        "(" -> {tokens ++ [{:text, current}, :open_paren], ""}
        ")" -> {tokens ++ [{:text, current}, :close_paren], ""}
        "," -> {tokens ++ [{:text, current}, :comma], ""}
        c -> {tokens, current <> c}
      end
    end)
    |> then(fn {tokens, current} ->
      if current == "", do: tokens, else: tokens ++ [{:text, current}]
    end)
    |> Enum.reject(fn
      {:text, ""} -> true
      _ -> false
    end)
  end

  defp parse_items([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_items([{:text, text} | rest], acc) do
    has_children = match?([:open_paren | _], rest)

    case parse_item_text(text, has_children) do
      {:ok, item, :simple} ->
        case rest do
          [:comma | rest2] -> parse_items(rest2, [item | acc])
          [] -> {:ok, Enum.reverse([item | acc])}
          [:open_paren | _] -> {:error, "unexpected '(' after field"}
          _ -> {:error, "unexpected token after field: #{inspect(rest)}"}
        end

      {:ok, item, :expects_children} ->
        case rest do
          [:open_paren | rest2] ->
            case parse_nested(rest2, []) do
              {:ok, children, rest3} ->
                item = %{item | children: children}

                case rest3 do
                  [:comma | rest4] -> parse_items(rest4, [item | acc])
                  [] -> {:ok, Enum.reverse([item | acc])}
                  _ -> {:error, "unexpected token after relation: #{inspect(rest3)}"}
                end

              {:error, _} = error ->
                error
            end

          _ ->
            {:error, "expected '(' after relation name"}
        end

      {:error, _} = error ->
        error
    end
  end

  defp parse_items(tokens, _acc), do: {:error, "unexpected tokens: #{inspect(tokens)}"}

  defp parse_nested([:close_paren | rest], acc), do: {:ok, Enum.reverse(acc), rest}
  defp parse_nested([], _acc), do: {:error, "unclosed parenthesis"}

  defp parse_nested([{:text, text} | rest], acc) do
    has_children = match?([:open_paren | _], rest)

    case parse_item_text(text, has_children) do
      {:ok, item, :simple} ->
        case rest do
          [:comma | rest2] -> parse_nested(rest2, [item | acc])
          [:close_paren | _] = rest2 -> parse_nested(rest2, [item | acc])
          [] -> {:error, "unclosed parenthesis"}
          [:open_paren | _] -> {:error, "unexpected '(' after field"}
          _ -> {:error, "unexpected token in nested select"}
        end

      {:ok, item, :expects_children} ->
        case rest do
          [:open_paren | rest2] ->
            case parse_nested(rest2, []) do
              {:ok, children, rest3} ->
                item = %{item | children: children}

                case rest3 do
                  [:comma | rest4] -> parse_nested(rest4, [item | acc])
                  [:close_paren | _] = rest4 -> parse_nested(rest4, [item | acc])
                  [] -> {:error, "unclosed parenthesis"}
                  _ -> {:error, "unexpected token after nested relation"}
                end

              {:error, _} = error ->
                error
            end

          _ ->
            {:error, "expected '(' after relation name"}
        end

      {:error, _} = error ->
        error
    end
  end

  defp parse_nested(tokens, _acc),
    do: {:error, "unexpected tokens in nested select: #{inspect(tokens)}"}

  defp parse_item_text(text, has_children) do
    {is_spread, text} = extract_spread_prefix(text)
    {alias_name, name_with_hint} = extract_alias(text)
    {name, hint} = extract_hint(name_with_hint)

    cond do
      String.contains?(name, "(") ->
        {:error, "invalid field name: #{name}"}

      name == "" ->
        {:error, "empty field name"}

      is_spread ->
        item = %SelectItem{
          type: :spread,
          name: name,
          alias: alias_name,
          hint: hint,
          children: nil
        }

        {:ok, item, :expects_children}

      has_children ->
        item = %SelectItem{
          type: :relation,
          name: name,
          alias: alias_name,
          hint: hint,
          children: nil
        }

        {:ok, item, :expects_children}

      true ->
        item = build_field_item(name, alias_name)
        {:ok, item, :simple}
    end
  end

  defp build_field_item(name, alias_name) do
    case FilterParser.parse_field(name) do
      {:ok, field} when field.json_path != [] ->
        %SelectItem{
          type: :field,
          name: field.name,
          alias: alias_name,
          children: nil,
          hint: {:json_path, field.json_path}
        }

      _ ->
        %SelectItem{type: :field, name: name, alias: alias_name, children: nil, hint: nil}
    end
  end

  defp extract_spread_prefix("..." <> rest), do: {true, rest}
  defp extract_spread_prefix(text), do: {false, text}

  defp extract_alias(text) do
    case String.split(text, ":", parts: 2) do
      [alias_name, rest] -> {alias_name, rest}
      [name] -> {nil, name}
    end
  end

  defp extract_hint(text) do
    case Regex.run(~r/^(.+?)!(.+)$/, text) do
      [_, name, hint] -> {name, hint}
      nil -> {text, nil}
    end
  end
end
