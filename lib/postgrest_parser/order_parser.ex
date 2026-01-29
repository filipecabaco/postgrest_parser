defmodule PostgrestParser.OrderParser do
  @moduledoc """
  Parses PostgREST order expressions into AST structures using NimbleParsec.

  Supports:
  - Simple ordering: id, name
  - Direction: id.asc, id.desc
  - Nulls handling: id.asc.nullsfirst, id.desc.nullslast
  - Multiple columns: id.desc,name.asc
  - JSON paths: data->key.desc
  """

  import NimbleParsec

  alias PostgrestParser.AST.OrderTerm
  alias PostgrestParser.Parsers.Common

  # Direction keywords
  direction =
    choice([
      string("desc") |> replace(:desc),
      string("asc") |> replace(:asc)
    ])

  # Nulls handling keywords
  nulls_option =
    choice([
      string("nullsfirst") |> replace(:first),
      string("nullslast") |> replace(:last)
    ])

  # Identifier for field names (allows alphanumeric and underscore)
  identifier_char = utf8_char([?a..?z, ?A..?Z, ?0..?9, ?_])

  identifier =
    times(identifier_char, min: 1)
    |> reduce({List, :to_string, []})

  # JSON operators: must try ->> before ->
  json_operator =
    choice([
      string("->>") |> replace(:double_arrow),
      string("->") |> replace(:arrow)
    ])

  # JSON path segment: operator + identifier
  json_path_segment =
    json_operator
    |> concat(identifier)
    |> reduce(:build_json_segment)

  # Field with optional JSON path
  field_with_json =
    identifier
    |> unwrap_and_tag(:name)
    |> concat(repeat(json_path_segment) |> tag(:json_path))

  # Order term: field[.direction][.nulls]
  order_term =
    field_with_json
    |> optional(
      ignore(string("."))
      |> concat(direction |> unwrap_and_tag(:direction))
    )
    |> optional(
      ignore(string("."))
      |> concat(nulls_option |> unwrap_and_tag(:nulls))
    )

  # Multiple order terms separated by comma
  order_list =
    order_term
    |> tag(:term)
    |> repeat(
      ignore(string(","))
      |> concat(order_term |> tag(:term))
    )

  defparsec(:parse_internal, order_list)

  @doc false
  def build_json_segment([op, key]), do: {op, key}

  @doc """
  Parses an order string into a list of OrderTerm structs.

  ## Examples

      iex> PostgrestParser.OrderParser.parse("id")
      {:ok, [%PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :asc, nulls: nil}]}

      iex> PostgrestParser.OrderParser.parse("id.desc")
      {:ok, [%PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :desc, nulls: nil}]}

      iex> PostgrestParser.OrderParser.parse("id.desc.nullsfirst")
      {:ok, [%PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :desc, nulls: :first}]}

      iex> PostgrestParser.OrderParser.parse("id.asc.nullslast")
      {:ok, [%PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :asc, nulls: :last}]}

      iex> PostgrestParser.OrderParser.parse("id.desc,name.asc")
      {:ok, [
        %PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :desc, nulls: nil},
        %PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "name", json_path: []}, direction: :asc, nulls: nil}
      ]}

      iex> PostgrestParser.OrderParser.parse("")
      {:ok, []}
  """
  @spec parse(String.t()) :: {:ok, [OrderTerm.t()]} | {:error, String.t()}
  def parse(""), do: {:ok, []}
  def parse(nil), do: {:ok, []}

  def parse(order_str) when is_binary(order_str) do
    case parse_internal(order_str) do
      {:ok, result, "", _, _, _} ->
        terms = extract_terms(result)
        {:ok, terms}

      {:ok, _result, rest, _, _, _} ->
        fallback_parse(order_str, "unparsed content: #{rest}")

      {:error, reason, _rest, _, _, _} ->
        fallback_parse(order_str, reason)
    end
  end

  defp extract_terms(parsed) do
    for {:term, term_data} <- parsed do
      build_order_term(term_data)
    end
  end

  defp build_order_term(term_data) do
    alias PostgrestParser.AST.Field

    %OrderTerm{
      field: %Field{
        name: Keyword.fetch!(term_data, :name),
        json_path: Keyword.get(term_data, :json_path, [])
      },
      direction: Keyword.get(term_data, :direction, :asc),
      nulls: Keyword.get(term_data, :nulls)
    }
  end

  defp fallback_parse(order_str, _reason) do
    order_str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reduce_while({:ok, []}, fn term_str, {:ok, acc} ->
      case parse_term(term_str) do
        {:ok, term} -> {:cont, {:ok, [term | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> then(fn
      {:ok, terms} -> {:ok, Enum.reverse(terms)}
      {:error, _} = error -> error
    end)
  end

  @directions ~w(asc desc)
  @nulls_options ~w(nullsfirst nullslast)

  @doc """
  Parses a single order term.

  ## Examples

      iex> PostgrestParser.OrderParser.parse_term("id")
      {:ok, %PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "id", json_path: []}, direction: :asc, nulls: nil}}

      iex> PostgrestParser.OrderParser.parse_term("data->key.desc")
      {:ok, %PostgrestParser.AST.OrderTerm{field: %PostgrestParser.AST.Field{name: "data", json_path: [{:arrow, "key"}]}, direction: :desc, nulls: nil}}
  """
  @spec parse_term(String.t()) :: {:ok, OrderTerm.t()} | {:error, String.t()}
  def parse_term(term_str) when is_binary(term_str) do
    parts = String.split(term_str, ".")

    case extract_field_and_options(parts) do
      {:ok, field_str, direction, nulls} ->
        with {:ok, field} <- Common.parse_field(field_str) |> handle_field_parse() do
          {:ok, %OrderTerm{field: field, direction: direction, nulls: nulls}}
        end

      {:error, _} = error ->
        error
    end
  end

  defp handle_field_parse({:ok, result, "", _, _, _}) do
    {:ok, Common.extract_field(result)}
  end

  defp handle_field_parse({:ok, result, rest, _, _, _}) do
    # Field has unparsed content (e.g., dots in field name)
    # Combine the parsed name with the rest
    field = Common.extract_field(result)
    {:ok, %{field | name: field.name <> rest}}
  end

  defp handle_field_parse({:error, _reason, _rest, _, _, _}) do
    # Let the fallback string parser handle this case
    {:error, :use_fallback}
  end

  defp extract_field_and_options(parts) do
    {field_parts, option_parts} = split_field_and_options(parts)

    field_str = Enum.join(field_parts, ".")

    case parse_options(option_parts) do
      {:ok, direction, nulls} -> {:ok, field_str, direction, nulls}
      {:error, _} = error -> error
    end
  end

  defp split_field_and_options(parts) do
    Enum.split_while(parts, fn part ->
      part not in @directions and part not in @nulls_options
    end)
  end

  defp parse_options([]), do: {:ok, :asc, nil}

  defp parse_options([direction]) when direction in @directions do
    {:ok, String.to_atom(direction), nil}
  end

  defp parse_options([nulls]) when nulls in @nulls_options do
    {:ok, :asc, parse_nulls(nulls)}
  end

  defp parse_options([direction, nulls])
       when direction in @directions and nulls in @nulls_options do
    {:ok, String.to_atom(direction), parse_nulls(nulls)}
  end

  defp parse_options(invalid) do
    {:error, "invalid order options: #{inspect(invalid)}"}
  end

  defp parse_nulls("nullsfirst"), do: :first
  defp parse_nulls("nullslast"), do: :last
end
