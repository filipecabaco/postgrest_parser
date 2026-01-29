defmodule PostgrestParser.Parsers.Common do
  @moduledoc """
  Shared NimbleParsec combinators used across all PostgREST parsers.

  Provides reusable building blocks for:
  - Identifiers (field names, type names, aliases)
  - JSON path operators (-> and ->>)
  - Type casting (::type)
  - Complete field parsing with JSON paths

  ## Usage

  Import this module's combinators into your parser:

      import NimbleParsec
      alias PostgrestParser.Parsers.Common

      # Use the parsed field helper
      Common.parse_field("data->key::text")
      # => {:ok, [name: "data", json_path: [{:arrow, "key"}], cast: "text"], "", ...}

  ## Shared Patterns

  All parsers in this library follow these conventions:
  - Identifiers: `[a-zA-Z0-9_]+`
  - JSON operators: `->` (arrow) and `->>` (double_arrow), always try `->>` first
  - Type casts: `::identifier`
  - Field structure: `identifier[json_path]*[::cast]`
  """

  import NimbleParsec

  # ===========================================================================
  # Basic Character Sets
  # ===========================================================================

  # Character set for identifiers: alphanumeric and underscore
  identifier_char = utf8_char([?a..?z, ?A..?Z, ?0..?9, ?_])

  # ===========================================================================
  # Identifier Parser
  # ===========================================================================

  # Parses an identifier: one or more alphanumeric/underscore characters.
  # Returns a string.
  defcombinatorp(
    :identifier,
    times(identifier_char, min: 1)
    |> reduce({List, :to_string, []})
  )

  # ===========================================================================
  # JSON Path Operators
  # ===========================================================================

  # JSON double arrow operator: ->> (must try before single arrow)
  json_double_arrow =
    string("->>")
    |> replace(:double_arrow)

  # JSON single arrow operator: ->
  json_single_arrow =
    string("->")
    |> replace(:arrow)

  # Parses a JSON operator: `->` or `->>`.
  # Returns `:arrow` or `:double_arrow` atom.
  # Always tries `->>` before `->` to ensure correct matching.
  defcombinatorp(
    :json_operator,
    choice([json_double_arrow, json_single_arrow])
  )

  # ===========================================================================
  # JSON Path Segment
  # ===========================================================================

  # Parses a JSON path segment: operator followed by identifier.
  # Returns a tuple like `{:arrow, "key"}` or `{:double_arrow, "value"}`.
  defcombinatorp(
    :json_path_segment,
    parsec(:json_operator)
    |> concat(parsec(:identifier))
    |> reduce(:build_json_segment)
  )

  @doc """
  Builds a JSON segment tuple from parser result.
  Called by `:json_path_segment` combinator.
  """
  def build_json_segment([op, key]), do: {op, key}

  # ===========================================================================
  # JSON Path (Multiple Segments)
  # ===========================================================================

  # Parses zero or more JSON path segments.
  # Returns a list of tuples.
  defcombinatorp(
    :json_path,
    repeat(parsec(:json_path_segment))
  )

  # ===========================================================================
  # Type Cast
  # ===========================================================================

  # Parses a type cast: `::identifier`.
  # Returns just the type name (string).
  defcombinatorp(
    :type_cast,
    ignore(string("::"))
    |> concat(parsec(:identifier))
  )

  # ===========================================================================
  # Field Name
  # ===========================================================================

  # Parses a field name (just the base column name).
  # Tags result as `:name`.
  defcombinatorp(
    :field_name,
    parsec(:identifier)
    |> unwrap_and_tag(:name)
  )

  # ===========================================================================
  # Complete Field Parser
  # ===========================================================================

  # Parses a complete field: `identifier[->path]*[::cast]`
  #
  # Returns a tagged keyword list:
  # - `:name` - the base field name (string)
  # - `:json_path` - list of JSON path tuples (may be empty)
  # - `:cast` - optional type cast (string or nil)
  defcombinatorp(
    :field,
    parsec(:field_name)
    |> concat(parsec(:json_path) |> tag(:json_path))
    |> concat(optional(parsec(:type_cast) |> unwrap_and_tag(:cast)))
  )

  # ===========================================================================
  # Public Parser Exports
  # ===========================================================================

  @doc """
  Parses an identifier string.

  ## Examples

      iex> PostgrestParser.Parsers.Common.parse_identifier("field_name")
      {:ok, ["field_name"], "", %{}, {1, 0}, 10}
  """
  defparsec(:parse_identifier, parsec(:identifier))

  @doc """
  Parses a JSON path (zero or more segments).

  ## Examples

      iex> PostgrestParser.Parsers.Common.parse_json_path("->key->>value")
      {:ok, [{:arrow, "key"}, {:double_arrow, "value"}], "", %{}, {1, 0}, 13}
  """
  defparsec(:parse_json_path, parsec(:json_path))

  @doc """
  Parses a complete field specification.

  ## Examples

      iex> PostgrestParser.Parsers.Common.parse_field("data->key::text")
      {:ok, [name: "data", json_path: [{:arrow, "key"}], cast: "text"], "", %{}, {1, 0}, 15}
  """
  defparsec(:parse_field, parsec(:field))

  # ===========================================================================
  # Result Extraction Helpers
  # ===========================================================================

  @doc """
  Extracts a Field struct from parsed field result.

  ## Examples

      iex> PostgrestParser.Parsers.Common.extract_field([name: "id", json_path: [], cast: nil])
      %PostgrestParser.AST.Field{name: "id", json_path: [], cast: nil}

      iex> PostgrestParser.Parsers.Common.extract_field([name: "data", json_path: [{:arrow, "key"}]])
      %PostgrestParser.AST.Field{name: "data", json_path: [{:arrow, "key"}], cast: nil}
  """
  @spec extract_field(keyword()) :: PostgrestParser.AST.Field.t()
  def extract_field(parsed) do
    alias PostgrestParser.AST.Field

    name = Keyword.fetch!(parsed, :name)
    json_path = Keyword.get(parsed, :json_path, [])
    cast = Keyword.get(parsed, :cast)

    %Field{name: name, json_path: json_path, cast: cast}
  end

  # ===========================================================================
  # Shared Validation and Transformation Functions
  # ===========================================================================

  @doc """
  Handles the common pattern of processing NimbleParsec field parse results.
  Returns `{:ok, Field.t()}` or `{:error, reason}`.

  ## Examples

      iex> result = PostgrestParser.Parsers.Common.parse_field("data->key")
      iex> PostgrestParser.Parsers.Common.handle_field_result(result)
      {:ok, %PostgrestParser.AST.Field{name: "data", json_path: [{:arrow, "key"}], cast: nil}}
  """
  @spec handle_field_result(tuple()) ::
          {:ok, PostgrestParser.AST.Field.t()} | {:error, String.t()}
  def handle_field_result({:ok, result, "", _, _, _}) do
    {:ok, extract_field(result)}
  end

  def handle_field_result({:ok, result, rest, _, _, _}) do
    # Field has unparsed content (e.g., dots in field name)
    # Combine the parsed name with the rest
    field = extract_field(result)
    {:ok, %{field | name: field.name <> rest}}
  end

  def handle_field_result({:error, _reason, _rest, _, _, _}) do
    {:error, :use_fallback}
  end
end
