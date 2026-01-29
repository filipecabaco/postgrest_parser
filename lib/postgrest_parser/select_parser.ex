defmodule PostgrestParser.SelectParser do
  @moduledoc """
  Parses PostgREST select expressions into AST structures using NimbleParsec.

  Supports:
  - Simple fields: name, id
  - Aliases: alias:name
  - Wildcards: *
  - Relations: client(id,name)
  - Spread relations: ...client(id)
  - Hints: client!inner(id)
  - JSON paths: data->key, data->>key
  """

  import NimbleParsec

  alias PostgrestParser.AST.SelectItem
  alias PostgrestParser.FilterParser
  alias PostgrestParser.SelectParser.AliasParser

  # Field name with optional alias (delegate to AliasParser for full handling)
  # For the tokenizer, we just capture text between delimiters
  field_text_char = utf8_char(not: ?(, not: ?), not: ?,)

  field_text =
    times(field_text_char, min: 1)
    |> reduce({List, :to_string, []})

  # Simple item: text (field or relation name with possible alias/hint)
  simple_item =
    field_text
    |> tag(:text)

  # Recursive definition for nested select items
  # We use defcombinatorp for internal recursive parsers

  defcombinatorp(
    :nested_content,
    parsec(:select_item)
    |> repeat(
      ignore(string(","))
      |> concat(parsec(:select_item))
    )
  )

  defcombinatorp(
    :relation_children,
    ignore(string("("))
    |> concat(parsec(:nested_content))
    |> ignore(string(")"))
    |> tag(:children)
  )

  # A select item can be a simple field or a relation with children
  defcombinatorp(
    :select_item,
    simple_item
    |> concat(optional(parsec(:relation_children)))
    |> tag(:item)
  )

  # Top-level select list
  select_list =
    parsec(:select_item)
    |> repeat(
      ignore(string(","))
      |> concat(parsec(:select_item))
    )

  defparsec(:parse_internal, select_list)

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
    case parse_internal(select_str) do
      {:ok, result, "", _, _, _} -> transform_result(result)
      {:ok, _result, _rest, _, _, _} -> fallback_parse(select_str)
      {:error, _reason, _rest, _, _, _} -> fallback_parse(select_str)
    end
  end

  def parse(""), do: {:ok, []}
  def parse(nil), do: {:ok, []}

  defp transform_result(items) do
    items
    |> Enum.map(&transform_item/1)
    |> collect_results()
  end

  defp transform_item({:item, item_data}) do
    text = get_text(item_data)
    children_data = Keyword.get(item_data, :children)
    has_children = not is_nil(children_data)

    with {:ok, item, item_type} <- parse_item_text(text, has_children) do
      attach_children(item, item_type, children_data)
    end
  end

  defp attach_children(item, :simple, _children_data), do: {:ok, item}

  defp attach_children(item, :expects_children, children_data) do
    case transform_children(children_data) do
      {:ok, children} -> {:ok, %{item | children: children}}
      {:error, _} = error -> error
    end
  end

  defp get_text(item_data) do
    case Keyword.get(item_data, :text) do
      [text] when is_binary(text) -> text
      text when is_binary(text) -> text
      _ -> ""
    end
  end

  defp transform_children(nil), do: {:ok, nil}

  defp transform_children(children_data) do
    children_data
    |> Enum.map(&transform_item/1)
    |> collect_results()
  end

  defp collect_results(results) do
    results
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, item}, {:ok, acc} -> {:cont, {:ok, [item | acc]}}
      {:error, _} = error, _acc -> {:halt, error}
    end)
    |> then(fn
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, _} = error -> error
    end)
  end

  # Fallback to original tokenization-based parsing
  defp fallback_parse(select_str) do
    select_str
    |> tokenize()
    |> parse_items([])
  end

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
      {:ok, item, :simple} -> handle_simple_item(item, rest, acc)
      {:ok, item, :expects_children} -> handle_relation_item(item, rest, acc)
      {:error, _} = error -> error
    end
  end

  defp parse_items(tokens, _acc), do: {:error, "unexpected tokens: #{inspect(tokens)}"}

  defp handle_simple_item(item, rest, acc) do
    case rest do
      [:comma | rest2] -> parse_items(rest2, [item | acc])
      [] -> {:ok, Enum.reverse([item | acc])}
      [:open_paren | _] -> {:error, "unexpected '(' after field"}
      _ -> {:error, "unexpected token after field: #{inspect(rest)}"}
    end
  end

  defp handle_relation_item(item, rest, acc) do
    case rest do
      [:open_paren | rest2] -> parse_relation_children(item, rest2, acc)
      _ -> {:error, "expected '(' after relation name"}
    end
  end

  defp parse_relation_children(item, rest, acc) do
    case parse_nested(rest, []) do
      {:ok, children, rest3} -> continue_after_relation(%{item | children: children}, rest3, acc)
      {:error, _} = error -> error
    end
  end

  defp continue_after_relation(item, rest, acc) do
    case rest do
      [:comma | rest4] -> parse_items(rest4, [item | acc])
      [] -> {:ok, Enum.reverse([item | acc])}
      _ -> {:error, "unexpected token after relation: #{inspect(rest)}"}
    end
  end

  defp parse_nested([:close_paren | rest], acc), do: {:ok, Enum.reverse(acc), rest}
  defp parse_nested([], _acc), do: {:error, "unclosed parenthesis"}

  defp parse_nested([{:text, text} | rest], acc) do
    has_children = match?([:open_paren | _], rest)

    case parse_item_text(text, has_children) do
      {:ok, item, :simple} -> handle_nested_simple(item, rest, acc)
      {:ok, item, :expects_children} -> handle_nested_relation(item, rest, acc)
      {:error, _} = error -> error
    end
  end

  defp parse_nested(tokens, _acc),
    do: {:error, "unexpected tokens in nested select: #{inspect(tokens)}"}

  defp handle_nested_simple(item, rest, acc) do
    case rest do
      [:comma | rest2] -> parse_nested(rest2, [item | acc])
      [:close_paren | _] = rest2 -> parse_nested(rest2, [item | acc])
      [] -> {:error, "unclosed parenthesis"}
      [:open_paren | _] -> {:error, "unexpected '(' after field"}
      _ -> {:error, "unexpected token in nested select"}
    end
  end

  defp handle_nested_relation(item, rest, acc) do
    case rest do
      [:open_paren | rest2] -> parse_nested_relation_children(item, rest2, acc)
      _ -> {:error, "expected '(' after relation name"}
    end
  end

  defp parse_nested_relation_children(item, rest, acc) do
    case parse_nested(rest, []) do
      {:ok, children, rest3} ->
        continue_nested_after_relation(%{item | children: children}, rest3, acc)

      {:error, _} = error ->
        error
    end
  end

  defp continue_nested_after_relation(item, rest, acc) do
    case rest do
      [:comma | rest4] -> parse_nested(rest4, [item | acc])
      [:close_paren | _] = rest4 -> parse_nested(rest4, [item | acc])
      [] -> {:error, "unclosed parenthesis"}
      _ -> {:error, "unexpected token after nested relation"}
    end
  end

  defp parse_item_text(text, has_children) do
    {is_spread, text} = extract_spread_prefix(text)
    {alias_name, name_with_hint} = extract_alias(text)
    {name, hint} = extract_hint(name_with_hint)

    build_item(name, alias_name, hint, is_spread, has_children)
  end

  defp build_item("", _alias_name, _hint, _is_spread, _has_children) do
    {:error, "empty field name"}
  end

  defp build_item(name, alias_name, hint, is_spread, has_children)
       when is_binary(name) do
    case String.contains?(name, "(") do
      true -> {:error, "invalid field name: #{name}"}
      false -> build_valid_item(name, alias_name, hint, is_spread, has_children)
    end
  end

  defp build_valid_item(name, alias_name, hint, true = _is_spread, _has_children) do
    item = %SelectItem{type: :spread, name: name, alias: alias_name, hint: hint, children: nil}
    {:ok, item, :expects_children}
  end

  defp build_valid_item(name, alias_name, hint, false, true = _has_children) do
    item = %SelectItem{type: :relation, name: name, alias: alias_name, hint: hint, children: nil}
    {:ok, item, :expects_children}
  end

  defp build_valid_item(name, alias_name, _hint, false, false) do
    item = build_field_item(name, alias_name)
    {:ok, item, :simple}
  end

  defp build_field_item(name, alias_name) do
    case FilterParser.parse_field(name) do
      {:ok, field} when field.json_path != [] and not is_nil(field.cast) ->
        %SelectItem{
          type: :field,
          name: field.name,
          alias: alias_name,
          children: nil,
          hint: {:json_path_cast, field.json_path, field.cast}
        }

      {:ok, field} when field.json_path != [] ->
        %SelectItem{
          type: :field,
          name: field.name,
          alias: alias_name,
          children: nil,
          hint: {:json_path, field.json_path}
        }

      {:ok, field} when not is_nil(field.cast) ->
        %SelectItem{
          type: :field,
          name: field.name,
          alias: alias_name,
          children: nil,
          hint: {:cast, field.cast}
        }

      _ ->
        %SelectItem{type: :field, name: name, alias: alias_name, children: nil, hint: nil}
    end
  end

  defp extract_spread_prefix("..." <> rest), do: {true, rest}
  defp extract_spread_prefix(text), do: {false, text}

  defp extract_alias(text), do: AliasParser.parse(text)

  defp extract_hint(text) do
    case Regex.run(~r/^(.+?)!(.+)$/, text) do
      [_, name, hint] -> {name, hint}
      nil -> {text, nil}
    end
  end
end
