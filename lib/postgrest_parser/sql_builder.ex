defmodule PostgrestParser.SqlBuilder do
  @moduledoc """
  Builds parameterized SQL queries from PostgREST AST structures.

  Generates safe, parameterized SQL using $1, $2, etc. placeholders
  to prevent SQL injection.
  """

  alias PostgrestParser.AST.Field
  alias PostgrestParser.AST.Filter
  alias PostgrestParser.AST.LogicTree
  alias PostgrestParser.AST.OrderTerm
  alias PostgrestParser.AST.ParsedParams
  alias PostgrestParser.AST.SelectItem
  alias PostgrestParser.RelationBuilder

  defmodule Query do
    @moduledoc """
    Represents a SQL query with its parameters.
    """
    @type t :: %__MODULE__{
            sql: iodata(),
            params: [term()],
            param_index: non_neg_integer()
          }

    defstruct sql: [], params: [], param_index: 0
  end

  @doc """
  Builds a complete SELECT query from parsed parameters.

  ## Examples

      iex> params = %PostgrestParser.AST.ParsedParams{
      ...>   select: [%PostgrestParser.AST.SelectItem{type: :field, name: "*"}],
      ...>   filters: [],
      ...>   order: [],
      ...>   limit: nil,
      ...>   offset: nil
      ...> }
      iex> PostgrestParser.SqlBuilder.build_select("users", params)
      {:ok, %{query: ~s(SELECT * FROM "users"), params: [], tables: ["users"]}}
  """
  @spec build_select(String.t(), ParsedParams.t()) :: {:ok, map()} | {:error, String.t()}
  def build_select(table, %ParsedParams{} = params) do
    query = %Query{}

    with {:ok, query} <- add_select_clause(query, params.select),
         {:ok, query} <- add_from_clause(query, table),
         {:ok, query} <- add_where_clause(query, params.filters),
         {:ok, query} <- add_order_clause(query, params.order),
         {:ok, query} <- add_limit_offset(query, params.limit, params.offset) do
      relation_tables = extract_relation_tables(params.select)
      all_tables = [table | relation_tables] |> Enum.uniq()
      result = finalize(query)
      {:ok, Map.put(result, :tables, all_tables)}
    end
  end

  @doc """
  Builds a SELECT query with embedded relations using LATERAL JOINs.

  Uses schema cache to resolve relationships and generate proper join SQL.

  ## Examples

      iex> _params = %PostgrestParser.AST.ParsedParams{
      ...>   select: [
      ...>     %PostgrestParser.AST.SelectItem{type: :field, name: "id"},
      ...>     %PostgrestParser.AST.SelectItem{type: :relation, name: "orders", children: [
      ...>       %PostgrestParser.AST.SelectItem{type: :field, name: "id"}
      ...>     ]}
      ...>   ],
      ...>   filters: [],
      ...>   order: [],
      ...>   limit: nil,
      ...>   offset: nil
      ...> }
      iex> # Returns query with LATERAL JOIN for orders relation
  """
  @spec build_select_with_relations(String.t(), String.t(), String.t(), ParsedParams.t()) ::
          {:ok, map()} | {:error, String.t()}
  def build_select_with_relations(tenant_id, schema, table, %ParsedParams{} = params) do
    select_items = params.select || [%SelectItem{type: :field, name: "*"}]

    {field_items, relation_items} =
      Enum.split_with(select_items, fn item -> item.type == :field end)

    if Enum.empty?(relation_items) do
      build_select(table, params)
    else
      with {:ok, {join_sql, relation_selects}} <-
             RelationBuilder.build_relation_joins(tenant_id, schema, table, relation_items) do
        build_select_with_joins(table, params, field_items, join_sql, relation_selects)
      end
    end
  end

  defp build_select_with_joins(table, params, field_items, join_sql, relation_selects) do
    query = %Query{}

    field_columns =
      if Enum.empty?(field_items) do
        "#{quote_identifier(table)}.*"
      else
        Enum.map_join(field_items, ", ", fn item ->
          "#{quote_identifier(table)}.#{select_item_to_sql(item)}"
        end)
      end

    select_columns =
      if relation_selects == "" do
        field_columns
      else
        "#{field_columns}, #{relation_selects}"
      end

    query = add_sql(query, ["SELECT ", select_columns])
    query = add_sql(query, [" FROM ", quote_identifier(table)])

    query =
      if join_sql != "" do
        add_sql(query, ["\n", join_sql])
      else
        query
      end

    with {:ok, query} <- add_where_clause(query, params.filters),
         {:ok, query} <- add_order_clause(query, params.order),
         {:ok, query} <- add_limit_offset(query, params.limit, params.offset) do
      relation_tables = extract_relation_tables(params.select)
      all_tables = [table | relation_tables] |> Enum.uniq()
      result = finalize(query)
      {:ok, Map.put(result, :tables, all_tables)}
    end
  end

  @doc """
  Builds a WHERE clause from a list of filters.

  ## Examples

      iex> filter = %PostgrestParser.AST.Filter{
      ...>   field: %PostgrestParser.AST.Field{name: "id", json_path: []},
      ...>   operator: :eq,
      ...>   value: "1",
      ...>   negated?: false
      ...> }
      iex> PostgrestParser.SqlBuilder.build_where_clause([filter])
      {:ok, %{clause: ~s("id" = $1), params: [1]}}
  """
  @spec build_where_clause([Filter.t() | LogicTree.t()]) :: {:ok, map()} | {:error, String.t()}
  def build_where_clause([]), do: {:ok, %{clause: "", params: []}}

  def build_where_clause(filters) do
    query = %Query{}

    case build_filter_list(query, filters, " AND ") do
      {:ok, query} ->
        {:ok, %{clause: IO.iodata_to_binary(query.sql), params: Enum.reverse(query.params)}}

      {:error, _} = error ->
        error
    end
  end

  defp add_select_clause(query, nil),
    do: add_select_clause(query, [%SelectItem{type: :field, name: "*"}])

  defp add_select_clause(query, []),
    do: add_select_clause(query, [%SelectItem{type: :field, name: "*"}])

  defp add_select_clause(query, items) do
    columns = Enum.map_join(items, ", ", &select_item_to_sql/1)
    {:ok, add_sql(query, ["SELECT ", columns])}
  end

  defp select_item_to_sql(%SelectItem{type: :field, name: "*"}), do: "*"

  defp select_item_to_sql(%SelectItem{type: :field, name: name, alias: nil, hint: nil}) do
    quote_identifier(name)
  end

  defp select_item_to_sql(%SelectItem{type: :field, name: name, alias: alias_name, hint: nil})
       when is_binary(alias_name) do
    "#{quote_identifier(name)} AS #{quote_identifier(alias_name)}"
  end

  defp select_item_to_sql(%SelectItem{
         type: :field,
         name: name,
         alias: alias_name,
         hint: {:json_path, path}
       }) do
    json_expr = build_json_path_sql(name, path)
    alias_part = if alias_name, do: " AS #{quote_identifier(alias_name)}", else: ""
    json_expr <> alias_part
  end

  defp select_item_to_sql(%SelectItem{type: :relation, name: name, children: children}) do
    child_columns = Enum.map_join(children, ", ", &select_item_to_sql/1)

    "(SELECT json_agg(row_to_json(#{quote_identifier(name)})) FROM #{quote_identifier(name)} WHERE #{child_columns})"
  end

  defp select_item_to_sql(%SelectItem{type: :spread}), do: "*"

  defp add_from_clause(query, table) do
    {:ok, add_sql(query, [" FROM ", quote_identifier(table)])}
  end

  defp add_where_clause(query, []), do: {:ok, query}

  defp add_where_clause(query, filters) do
    query = add_sql(query, [" WHERE "])
    build_filter_list(query, filters, " AND ")
  end

  defp build_filter_list(query, filters, joiner) do
    filters
    |> Enum.reduce_while({:ok, query, []}, fn filter, {:ok, q, clauses} ->
      case build_single_filter(q, filter) do
        {:ok, q, clause} -> {:cont, {:ok, q, [clause | clauses]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, query, clauses} ->
        joined = clauses |> Enum.reverse() |> Enum.join(joiner)
        {:ok, add_sql(query, [joined])}

      {:error, _} = error ->
        error
    end
  end

  defp build_single_filter(query, %Filter{} = filter) do
    field_sql = field_to_sql(filter.field)

    {query, clause} =
      operator_to_sql(query, field_sql, filter.operator, filter.value, filter.negated?)

    {:ok, query, clause}
  end

  defp build_single_filter(query, %LogicTree{} = tree) do
    joiner = if tree.operator == :and, do: " AND ", else: " OR "

    case build_logic_tree_conditions(query, tree.conditions, joiner) do
      {:ok, query, conditions_sql} ->
        clause =
          if tree.negated?,
            do: "NOT (#{conditions_sql})",
            else: "(#{conditions_sql})"

        {:ok, query, clause}

      {:error, _} = error ->
        error
    end
  end

  defp build_logic_tree_conditions(query, conditions, joiner) do
    conditions
    |> Enum.reduce_while({:ok, query, []}, fn condition, {:ok, q, clauses} ->
      case build_single_filter(q, condition) do
        {:ok, q, clause} -> {:cont, {:ok, q, [clause | clauses]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, query, clauses} ->
        joined = clauses |> Enum.reverse() |> Enum.join(joiner)
        {:ok, query, joined}

      {:error, _} = error ->
        error
    end
  end

  defp field_to_sql(%Field{name: name, json_path: []}), do: quote_identifier(name)
  defp field_to_sql(%Field{name: name, json_path: path}), do: build_json_path_sql(name, path)
  defp field_to_sql(name) when is_binary(name), do: quote_identifier(name)

  defp build_json_path_sql(name, path) do
    path_sql =
      Enum.map_join(path, "", fn
        {:arrow, key} -> "->#{quote_literal(key)}"
        {:double_arrow, key} -> "->>#{quote_literal(key)}"
        {:array_index, idx} -> "->#{idx}"
      end)

    "#{quote_identifier(name)}#{path_sql}"
  end

  defp operator_to_sql(query, field, :eq, value, negated?) do
    op = if negated?, do: "<>", else: "="
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{op} #{param_ref}"}
  end

  defp operator_to_sql(query, field, :neq, value, negated?) do
    op = if negated?, do: "=", else: "<>"
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{op} #{param_ref}"}
  end

  defp operator_to_sql(query, field, :gt, value, negated?) do
    op = if negated?, do: "<=", else: ">"
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{op} #{param_ref}"}
  end

  defp operator_to_sql(query, field, :gte, value, negated?) do
    op = if negated?, do: "<", else: ">="
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{op} #{param_ref}"}
  end

  defp operator_to_sql(query, field, :lt, value, negated?) do
    op = if negated?, do: ">=", else: "<"
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{op} #{param_ref}"}
  end

  defp operator_to_sql(query, field, :lte, value, negated?) do
    op = if negated?, do: ">", else: "<="
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{op} #{param_ref}"}
  end

  defp operator_to_sql(query, field, :like, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{not_prefix}LIKE #{param_ref}"}
  end

  defp operator_to_sql(query, field, :ilike, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{not_prefix}ILIKE #{param_ref}"}
  end

  defp operator_to_sql(query, field, :match, value, negated?) do
    not_prefix = if negated?, do: "!", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{not_prefix}~ #{param_ref}"}
  end

  defp operator_to_sql(query, field, :imatch, value, negated?) do
    not_prefix = if negated?, do: "!", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{field} #{not_prefix}~* #{param_ref}"}
  end

  defp operator_to_sql(query, field, :in, values, negated?) when is_list(values) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, values)
    {query, "#{field} #{not_prefix}= ANY(#{param_ref})"}
  end

  defp operator_to_sql(query, field, :is, value, negated?) do
    clause = build_is_clause(field, value, negated?)
    {query, clause}
  end

  defp operator_to_sql(query, field, :fts, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} @@ to_tsquery(#{param_ref})"}
  end

  defp operator_to_sql(query, field, :plfts, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} @@ plainto_tsquery(#{param_ref})"}
  end

  defp operator_to_sql(query, field, :phfts, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} @@ phraseto_tsquery(#{param_ref})"}
  end

  defp operator_to_sql(query, field, :wfts, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} @@ websearch_to_tsquery(#{param_ref})"}
  end

  defp operator_to_sql(query, field, :cs, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} @> #{param_ref}"}
  end

  defp operator_to_sql(query, field, :cd, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} <@ #{param_ref}"}
  end

  defp operator_to_sql(query, field, :ov, values, negated?) when is_list(values) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, values)
    {query, "#{not_prefix}#{field} && #{param_ref}"}
  end

  defp operator_to_sql(query, field, :sl, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} << #{param_ref}"}
  end

  defp operator_to_sql(query, field, :sr, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} >> #{param_ref}"}
  end

  defp operator_to_sql(query, field, :nxl, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} &< #{param_ref}"}
  end

  defp operator_to_sql(query, field, :nxr, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} &> #{param_ref}"}
  end

  defp operator_to_sql(query, field, :adj, value, negated?) do
    not_prefix = if negated?, do: "NOT ", else: ""
    {query, param_ref} = add_param(query, value)
    {query, "#{not_prefix}#{field} -|- #{param_ref}"}
  end

  defp build_is_clause(field, "null", false), do: "#{field} IS NULL"
  defp build_is_clause(field, "null", true), do: "#{field} IS NOT NULL"
  defp build_is_clause(field, "not_null", false), do: "#{field} IS NOT NULL"
  defp build_is_clause(field, "not_null", true), do: "#{field} IS NULL"
  defp build_is_clause(field, "true", false), do: "#{field} IS TRUE"
  defp build_is_clause(field, "true", true), do: "#{field} IS NOT TRUE"
  defp build_is_clause(field, "false", false), do: "#{field} IS FALSE"
  defp build_is_clause(field, "false", true), do: "#{field} IS NOT FALSE"
  defp build_is_clause(field, "unknown", false), do: "#{field} IS UNKNOWN"
  defp build_is_clause(field, "unknown", true), do: "#{field} IS NOT UNKNOWN"

  defp add_order_clause(query, []), do: {:ok, query}

  defp add_order_clause(query, order_terms) do
    clauses = Enum.map_join(order_terms, ", ", &order_term_to_sql/1)
    {:ok, add_sql(query, [" ORDER BY ", clauses])}
  end

  defp order_term_to_sql(%OrderTerm{field: field, direction: direction, nulls: nulls}) do
    field_sql = field_to_sql(field)
    dir_sql = if direction == :desc, do: " DESC", else: " ASC"

    nulls_sql =
      case nulls do
        :first -> " NULLS FIRST"
        :last -> " NULLS LAST"
        nil -> ""
      end

    "#{field_sql}#{dir_sql}#{nulls_sql}"
  end

  defp add_limit_offset(query, nil, nil), do: {:ok, query}

  defp add_limit_offset(query, limit, nil) when is_integer(limit) do
    {query, param_ref} = add_param(query, limit)
    {:ok, add_sql(query, [" LIMIT ", param_ref])}
  end

  defp add_limit_offset(query, nil, offset) when is_integer(offset) do
    {query, param_ref} = add_param(query, offset)
    {:ok, add_sql(query, [" OFFSET ", param_ref])}
  end

  defp add_limit_offset(query, limit, offset) when is_integer(limit) and is_integer(offset) do
    {query, limit_ref} = add_param(query, limit)
    {query, offset_ref} = add_param(query, offset)
    {:ok, add_sql(query, [" LIMIT ", limit_ref, " OFFSET ", offset_ref])}
  end

  defp extract_relation_tables(nil), do: []
  defp extract_relation_tables([]), do: []

  defp extract_relation_tables(select_items) do
    select_items
    |> Enum.flat_map(&extract_tables_from_item/1)
    |> Enum.uniq()
  end

  defp extract_tables_from_item(%SelectItem{type: :relation, name: name, children: children}) do
    child_tables = if children, do: extract_relation_tables(children), else: []
    [name | child_tables]
  end

  defp extract_tables_from_item(%SelectItem{type: :spread, name: name, children: children}) do
    child_tables = if children, do: extract_relation_tables(children), else: []
    [name | child_tables]
  end

  defp extract_tables_from_item(_), do: []

  defp add_sql(%Query{sql: sql} = query, parts) do
    %{query | sql: [sql | parts]}
  end

  defp add_param(%Query{params: params, param_index: idx} = query, value) do
    new_idx = idx + 1
    coerced = coerce_value(value)
    {%{query | params: [coerced | params], param_index: new_idx}, "$#{new_idx}"}
  end

  defp coerce_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        case Float.parse(value) do
          {_float, ""} -> Decimal.new(value)
          _ -> value
        end
    end
  end

  defp coerce_value(values) when is_list(values), do: Enum.map(values, &coerce_value/1)
  defp coerce_value(value), do: value

  defp finalize(%Query{sql: sql, params: params}) do
    %{
      query: IO.iodata_to_binary(sql),
      params: Enum.reverse(params)
    }
  end

  defp quote_identifier(name) do
    escaped = String.replace(name, "\"", "\"\"")
    "\"#{escaped}\""
  end

  defp quote_literal(value) do
    escaped = String.replace(value, "'", "''")
    "'#{escaped}'"
  end
end
