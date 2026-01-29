defmodule PostgrestParser.OrderParser do
  @moduledoc """
  Parses PostgREST order expressions into AST structures.

  Supports:
  - Simple ordering: id, name
  - Direction: id.asc, id.desc
  - Nulls handling: id.asc.nullsfirst, id.desc.nullslast
  - Multiple columns: id.desc,name.asc
  """

  alias PostgrestParser.AST.OrderTerm
  alias PostgrestParser.FilterParser

  @directions ~w(asc desc)
  @nulls_options ~w(nullsfirst nullslast)

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
    order_str
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.reduce_while({:ok, []}, fn term_str, {:ok, acc} ->
      case parse_term(term_str) do
        {:ok, term} -> {:cont, {:ok, [term | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, terms} -> {:ok, Enum.reverse(terms)}
      {:error, _} = error -> error
    end
  end

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
        with {:ok, field} <- FilterParser.parse_field(field_str) do
          {:ok, %OrderTerm{field: field, direction: direction, nulls: nulls}}
        end

      {:error, _} = error ->
        error
    end
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
