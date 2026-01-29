defmodule PostgrestParser.RelationBuilder do
  @moduledoc """
  Builds SQL for embedded relations using LATERAL JOINs.

  Generates PostgREST-style queries that return related data as JSON:
  - One-to-Many: json_agg() for arrays
  - Many-to-One: row_to_json() for objects
  - Many-to-Many: Through junction table
  """

  alias PostgrestParser.AST.SelectItem
  alias PostgrestParser.SchemaCache
  alias PostgrestParser.SchemaCache.Relationship

  defmodule RelationContext do
    @moduledoc "Context for building relation queries."
    defstruct [:tenant_id, :schema, :parent_alias, :depth, :relationships]
  end

  @doc """
  Builds LATERAL JOIN clauses for embedded relations in a select.

  Returns:
  - `{:ok, {join_clauses, select_additions}}` on success
  - `{:error, reason}` if relationship not found
  """
  @spec build_relation_joins(String.t(), String.t(), String.t(), [SelectItem.t()]) ::
          {:ok, {String.t(), String.t()}} | {:error, String.t()}
  def build_relation_joins(tenant_id, schema, table, select_items) do
    context = %RelationContext{
      tenant_id: tenant_id,
      schema: schema,
      parent_alias: table,
      depth: 0,
      relationships: SchemaCache.get_relationships(tenant_id, schema, table)
    }

    relations = Enum.filter(select_items, fn item -> item.type in [:relation, :spread] end)

    case build_joins_for_relations(relations, context) do
      {:ok, {joins, selects}} ->
        join_sql = Enum.join(joins, "\n")
        select_sql = Enum.join(selects, ", ")
        {:ok, {join_sql, select_sql}}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Builds a single relation join given relationship metadata.
  """
  @spec build_single_relation_join(SelectItem.t(), Relationship.t(), String.t(), integer()) ::
          {String.t(), String.t()}
  def build_single_relation_join(select_item, relationship, parent_alias, depth) do
    rel_alias = "#{select_item.name}_#{depth}"
    target_table = quote_identifier(relationship.target_table)
    child_columns = build_child_select(select_item.children, rel_alias)
    join_condition = build_join_condition(relationship, parent_alias, rel_alias)

    json_select =
      if child_columns == "*" do
        "#{rel_alias}.*"
      else
        "#{rel_alias}"
      end

    case relationship.cardinality do
      cardinality when cardinality in [:o2m, :m2m] ->
        join_sql = """
        LEFT JOIN LATERAL (
          SELECT json_agg(#{json_select}) AS #{rel_alias}
          FROM #{quote_identifier(relationship.target_schema)}.#{target_table} AS #{rel_alias}
          WHERE #{join_condition}
        ) AS #{rel_alias}_agg ON true
        """

        select_sql =
          "#{rel_alias}_agg.#{rel_alias} AS #{quote_identifier(select_item.alias || select_item.name)}"

        {String.trim(join_sql), select_sql}

      cardinality when cardinality in [:m2o, :o2o] ->
        join_sql = """
        LEFT JOIN LATERAL (
          SELECT row_to_json(#{json_select}) AS #{rel_alias}
          FROM #{quote_identifier(relationship.target_schema)}.#{target_table} AS #{rel_alias}
          WHERE #{join_condition}
          LIMIT 1
        ) AS #{rel_alias}_agg ON true
        """

        select_sql =
          "#{rel_alias}_agg.#{rel_alias} AS #{quote_identifier(select_item.alias || select_item.name)}"

        {String.trim(join_sql), select_sql}
    end
  end

  @doc """
  Builds SQL for a Many-to-Many relation through a junction table.
  """
  @spec build_m2m_join(SelectItem.t(), Relationship.t(), String.t(), integer()) ::
          {String.t(), String.t()}
  def build_m2m_join(select_item, relationship, parent_alias, depth) do
    rel_alias = "#{select_item.name}_#{depth}"
    junction = relationship.junction

    target_table = quote_identifier(relationship.target_table)
    junction_table = quote_identifier(junction.table)

    parent_join =
      build_column_pairs(
        relationship.source_columns,
        junction.source_columns,
        parent_alias,
        "junction_#{depth}"
      )

    target_join =
      build_column_pairs(
        junction.target_columns,
        relationship.target_columns,
        "junction_#{depth}",
        rel_alias
      )

    join_sql = """
    LEFT JOIN LATERAL (
      SELECT json_agg(#{rel_alias}.*) AS #{rel_alias}
      FROM #{quote_identifier(junction.schema)}.#{junction_table} AS junction_#{depth}
      JOIN #{quote_identifier(relationship.target_schema)}.#{target_table} AS #{rel_alias}
        ON #{target_join}
      WHERE #{parent_join}
    ) AS #{rel_alias}_agg ON true
    """

    select_sql =
      "#{rel_alias}_agg.#{rel_alias} AS #{quote_identifier(select_item.alias || select_item.name)}"

    {String.trim(join_sql), select_sql}
  end

  defp build_joins_for_relations(relations, context) do
    relations
    |> Enum.reduce_while({:ok, {[], []}}, fn relation, {:ok, {joins, selects}} ->
      case find_relationship(relation, context) do
        {:ok, relationship} ->
          {join_sql, select_sql} =
            if relationship.cardinality == :m2m do
              build_m2m_join(relation, relationship, context.parent_alias, context.depth)
            else
              build_single_relation_join(
                relation,
                relationship,
                context.parent_alias,
                context.depth
              )
            end

          {:cont, {:ok, {[join_sql | joins], [select_sql | selects]}}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, {joins, selects}} -> {:ok, {Enum.reverse(joins), Enum.reverse(selects)}}
      error -> error
    end
  end

  defp find_relationship(relation, context) do
    hint = relation.hint

    result =
      if hint do
        SchemaCache.find_relationship_with_hint(
          context.tenant_id,
          context.schema,
          context.parent_alias,
          relation.name,
          hint
        )
      else
        SchemaCache.find_relationship(
          context.tenant_id,
          context.schema,
          context.parent_alias,
          relation.name
        )
      end

    case result do
      {:ok, rel} -> {:ok, rel}
      {:error, :not_found} -> {:error, "relationship '#{relation.name}' not found"}
      {:error, :ambiguous} -> {:error, "relationship '#{relation.name}' is ambiguous, use hint"}
    end
  end

  defp build_join_condition(relationship, parent_alias, rel_alias) do
    build_column_pairs(
      relationship.source_columns,
      relationship.target_columns,
      parent_alias,
      rel_alias
    )
  end

  defp build_column_pairs(source_cols, target_cols, source_alias, target_alias) do
    Enum.zip(source_cols, target_cols)
    |> Enum.map_join(" AND ", fn {src, tgt} ->
      "#{quote_identifier(source_alias)}.#{quote_identifier(src)} = #{quote_identifier(target_alias)}.#{quote_identifier(tgt)}"
    end)
  end

  defp build_child_select(nil, _alias), do: "*"
  defp build_child_select([], _alias), do: "*"

  defp build_child_select(children, rel_alias) do
    children
    |> Enum.filter(fn c -> c.type == :field end)
    |> Enum.map(fn c ->
      "#{quote_identifier(rel_alias)}.#{quote_identifier(c.name)}"
    end)
    |> case do
      [] -> "*"
      cols -> Enum.join(cols, ", ")
    end
  end

  defp quote_identifier(name) when is_binary(name) do
    escaped = String.replace(name, "\"", "\"\"")
    "\"#{escaped}\""
  end
end
