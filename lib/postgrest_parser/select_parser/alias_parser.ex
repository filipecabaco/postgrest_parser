defmodule PostgrestParser.SelectParser.AliasParser do
  @moduledoc """
  Parser for PostgREST SELECT field syntax with alias support using NimbleParsec.

  This parser uses a hybrid approach combining NimbleParsec parser combinators
  with fallback string-based parsing to ensure 100% backward compatibility.

  ## Supported Syntax

  - Simple: `field_name`
  - With alias: `alias_name:field_name`
  - With cast: `field_name::type`
  - With cast and alias: `field_name::type:alias_name`
  - JSON path: `field->path`, `field->>path`
  - JSON path with cast: `field->path::type`
  - JSON path with cast and alias: `field->path::type:alias_name`
  - Special characters: `*`, `...`, hints (`!inner`)

  ## Implementation Strategy

  The parser tries NimbleParsec combinators first for structured inputs,
  then falls back to string manipulation for edge cases (hints, special chars).
  This provides the benefits of parser combinators while maintaining compatibility.
  """

  import NimbleParsec

  @type alias_name :: String.t() | nil
  @type field_name :: String.t()
  @type parse_result :: {alias_name(), field_name()}

  # Field name characters: alphanumeric, underscore, and special chars like *, ., !
  # We need to be careful not to consume : or -> or ->>
  field_char =
    utf8_char([
      ?a..?z,
      ?A..?Z,
      ?0..?9,
      ?_,
      ?*,
      ?.,
      ?!
    ])

  # Basic identifier: alphanumeric and underscore only (for aliases and type names)
  identifier_char = utf8_char([?a..?z, ?A..?Z, ?0..?9, ?_])

  identifier =
    times(identifier_char, min: 1)
    |> reduce({List, :to_string, []})

  # JSON path operators: ->> or -> (must try ->> first)
  json_operator = choice([string("->>"), string("->")])

  # JSON path segment: operator + identifier
  json_path_segment =
    json_operator
    |> concat(identifier)
    |> reduce({Enum, :join, [""]})

  # Type cast: ::type_name
  type_cast =
    string("::")
    |> concat(identifier)
    |> reduce({Enum, :join, [""]})

  # Field name: identifier + optional json paths
  field_name =
    identifier
    |> repeat(json_path_segment)
    |> reduce({Enum, :join, [""]})

  # Special field: wildcard (*) or spread (...identifier) or other special chars
  special_field =
    choice([
      string("*"),
      string("...")
      |> concat(
        repeat(field_char)
        |> reduce({List, :to_string, []})
      )
      |> reduce({Enum, :join, [""]}),
      times(field_char, min: 1)
      |> reduce({List, :to_string, []})
    ])

  # Try to parse as structured field first, fallback to special field
  any_field = choice([field_name, special_field])

  # Pattern 1: field[->path]::cast:alias (alias comes AFTER cast, REQUIRES cast)
  field_with_cast_and_alias =
    any_field
    # Make cast REQUIRED, not optional
    |> concat(type_cast)
    |> reduce({Enum, :join, [""]})
    |> tag(:field)
    |> ignore(string(":"))
    |> concat(
      identifier
      |> tag(:alias)
    )

  # Pattern 2: alias:field[->path] (NO cast, alias comes BEFORE)
  alias_then_field =
    identifier
    |> tag(:alias)
    |> ignore(string(":"))
    |> concat(
      any_field
      |> reduce({Enum, :join, [""]})
      |> tag(:field)
    )

  # Pattern 3: field[->path][::cast] (no alias at all)
  field_only =
    any_field
    |> optional(type_cast)
    |> reduce({Enum, :join, [""]})
    |> tag(:field)

  # Main parser: try patterns in order
  # 1. Try field with cast and alias (field::cast:alias) - has cast AND alias
  # 2. Try alias before field (alias:field) - NO cast, has alias
  # 3. Try field only (field or field::cast) - NO alias
  defparsec(
    :parse_internal,
    choice([
      field_with_cast_and_alias,
      alias_then_field,
      field_only
    ])
  )

  @doc """
  Parse a SELECT field item with optional alias.

  ## Parameters

    * `text` - The field specification string to parse

  ## Returns

  A tuple `{alias_name, field_name}` where:
    * `alias_name` - The alias if present, `nil` otherwise
    * `field_name` - The field name (may include JSON paths, casts, hints)

  ## Examples

      iex> PostgrestParser.SelectParser.AliasParser.parse("name")
      {nil, "name"}

      iex> PostgrestParser.SelectParser.AliasParser.parse("user_name:name")
      {"user_name", "name"}

      iex> PostgrestParser.SelectParser.AliasParser.parse("price::text:price_str")
      {"price_str", "price::text"}

      iex> PostgrestParser.SelectParser.AliasParser.parse("my_alias:data->value")
      {"my_alias", "data->value"}

      iex> PostgrestParser.SelectParser.AliasParser.parse("data->price::numeric:total")
      {"total", "data->price::numeric"}

      iex> PostgrestParser.SelectParser.AliasParser.parse("*")
      {nil, "*"}

      iex> PostgrestParser.SelectParser.AliasParser.parse("all:*")
      {"all", "*"}

  """
  @spec parse(String.t()) :: parse_result()
  def parse(text) when is_binary(text) and byte_size(text) > 0 do
    case parse_internal(text) do
      {:ok, result, "", _, _, _} ->
        # Successfully parsed entire string
        extract_result(result)

      {:ok, _result, _rest, _, _, _} ->
        # Parsed partially but left some text unparsed
        # Fall back to string-based approach for full compatibility
        fallback_parse(text)

      {:error, _reason, _rest, _, _, _} ->
        # If NimbleParsec fails, fall back to the original string-based approach
        # This handles edge cases and ensures backward compatibility
        fallback_parse(text)
    end
  end

  @spec parse(String.t()) :: parse_result()
  def parse(""), do: {nil, ""}

  # Extract alias and field from NimbleParsec result
  @spec extract_result(list()) :: parse_result()
  defp extract_result(parsed) when is_list(parsed) do
    # Handle different parse result structures
    case parsed do
      # Pattern 1: field with cast and alias - [{:field, [field]}, {:alias, [alias]}]
      [{:field, [field]}, {:alias, [alias]}] when is_binary(field) and is_binary(alias) ->
        {alias, field}

      # Pattern 2: alias before field - [{:alias, [alias]}, {:field, [field]}]
      [{:alias, [alias]}, {:field, [field]}] when is_binary(alias) and is_binary(field) ->
        {alias, field}

      # Pattern 3: field only - [field: [field_str]]
      [field: [field]] when is_binary(field) ->
        {nil, field}

      # Fallback: try extracting using Keyword
      _ ->
        extract_from_keyword(parsed)
    end
  end

  # Extract values from keyword list when structured extraction fails
  @spec extract_from_keyword(list()) :: parse_result()
  defp extract_from_keyword(parsed) do
    alias_value = Keyword.get(parsed, :alias)
    field_value = Keyword.get(parsed, :field)

    alias_name = normalize_value(alias_value)
    field_name = normalize_value(field_value) || ""

    {alias_name, field_name}
  end

  # Normalize parsed values from NimbleParsec result
  @spec normalize_value(term()) :: String.t() | nil
  defp normalize_value([value]) when is_binary(value), do: value
  defp normalize_value(value) when is_binary(value), do: value
  defp normalize_value(nil), do: nil
  defp normalize_value(_), do: nil

  # Fallback to string-based parsing for cases NimbleParsec can't handle
  # This handles edge cases like hints (!inner), spread operators (...), etc.
  @spec fallback_parse(String.t()) :: parse_result()
  defp fallback_parse(text) do
    has_cast = String.contains?(text, "::")
    protected = String.replace(text, "::", "\x00\x00")

    case :binary.matches(protected, ":") do
      [] ->
        field = String.replace(protected, "\x00\x00", "::")
        {nil, field}

      matches ->
        {pos, _len} = List.last(matches)
        before_colon = binary_part(protected, 0, pos)
        after_colon = binary_part(protected, pos + 1, byte_size(protected) - pos - 1)

        before_part = String.replace(before_colon, "\x00\x00", "::")
        after_part = String.replace(after_colon, "\x00\x00", "::")

        {alias_part, field_part} =
          if has_cast do
            {after_part, before_part}
          else
            {before_part, after_part}
          end

        {if(alias_part == "", do: alias_part, else: alias_part), field_part}
    end
  end
end
