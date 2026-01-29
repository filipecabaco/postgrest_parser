defmodule PostgrestParser.SchemaCache do
  @moduledoc """
  PostgreSQL schema metadata cache for PostgREST query building.

  Queries pg_catalog to discover:
  - Tables and columns (excluding partition children)
  - Foreign key relationships
  - Primary/unique key constraints

  Uses ETS for fast concurrent reads with GenServer for cache updates.
  """

  use GenServer

  alias PostgrestParser.SchemaCache.Column
  alias PostgrestParser.SchemaCache.Relationship
  alias PostgrestParser.SchemaCache.Table

  @ets_table :postgrest_schema_cache
  @ets_relationships :postgrest_relationships_cache

  defmodule Table do
    @moduledoc "Represents a database table with its columns."
    @type t :: %__MODULE__{
            schema: String.t(),
            name: String.t(),
            columns: [Column.t()],
            primary_key: [String.t()],
            is_view: boolean()
          }

    defstruct [:schema, :name, columns: [], primary_key: [], is_view: false]
  end

  defmodule Column do
    @moduledoc "Represents a database column."
    @type t :: %__MODULE__{
            name: String.t(),
            type: String.t(),
            nullable: boolean(),
            has_default: boolean(),
            position: non_neg_integer()
          }

    defstruct [:name, :type, :nullable, :has_default, :position]
  end

  defmodule Relationship do
    @moduledoc """
    Represents a foreign key relationship between tables.

    Cardinalities:
    - :m2o (Many-to-One): FK on source table pointing to target
    - :o2m (One-to-Many): Inverse of M2O
    - :o2o (One-to-One): FK columns are also primary/unique key
    - :m2m (Many-to-Many): Through junction table with two FKs
    """
    @type cardinality :: :m2o | :o2m | :o2o | :m2m

    @type t :: %__MODULE__{
            constraint_name: String.t(),
            source_schema: String.t(),
            source_table: String.t(),
            source_columns: [String.t()],
            target_schema: String.t(),
            target_table: String.t(),
            target_columns: [String.t()],
            cardinality: cardinality(),
            junction: map() | nil
          }

    defstruct [
      :constraint_name,
      :source_schema,
      :source_table,
      :source_columns,
      :target_schema,
      :target_table,
      :target_columns,
      :cardinality,
      :junction
    ]
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets table metadata from cache.

  ## Examples

      iex> SchemaCache.get_table("tenant_123", "public", "users")
      {:ok, %Table{name: "users", columns: [...]}}
  """
  @spec get_table(String.t(), String.t(), String.t()) :: {:ok, Table.t()} | {:error, :not_found}
  def get_table(tenant_id, schema, table) do
    case :ets.lookup(@ets_table, {tenant_id, schema, table}) do
      [{_, table_info}] -> {:ok, table_info}
      [] -> {:error, :not_found}
    end
  rescue
    ArgumentError -> {:error, :not_found}
  end

  @doc """
  Gets all relationships for a table.
  """
  @spec get_relationships(String.t(), String.t(), String.t()) :: [Relationship.t()]
  def get_relationships(tenant_id, schema, table) do
    case :ets.lookup(@ets_relationships, {tenant_id, schema, table}) do
      [{_, relationships}] -> relationships
      [] -> []
    end
  rescue
    ArgumentError -> []
  end

  @doc """
  Finds a specific relationship by target table name.
  """
  @spec find_relationship(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, Relationship.t()} | {:error, :not_found}
  def find_relationship(tenant_id, schema, source_table, target_table) do
    relationships = get_relationships(tenant_id, schema, source_table)

    case Enum.find(relationships, fn r -> r.target_table == target_table end) do
      nil -> {:error, :not_found}
      rel -> {:ok, rel}
    end
  end

  @doc """
  Finds relationship with disambiguation hint (constraint name or column).
  """
  @spec find_relationship_with_hint(String.t(), String.t(), String.t(), String.t(), String.t()) ::
          {:ok, Relationship.t()} | {:error, :not_found | :ambiguous}
  def find_relationship_with_hint(tenant_id, schema, source_table, target_table, hint) do
    relationships = get_relationships(tenant_id, schema, source_table)

    matches =
      Enum.filter(relationships, fn r ->
        r.target_table == target_table and
          (r.constraint_name == hint or hint in r.source_columns or hint in r.target_columns)
      end)

    case matches do
      [rel] -> {:ok, rel}
      [] -> {:error, :not_found}
      _multiple -> {:error, :ambiguous}
    end
  end

  @doc """
  Refreshes schema cache for a tenant using the provided connection.
  """
  @spec refresh(String.t(), pid() | DBConnection.conn()) :: :ok | {:error, term()}
  def refresh(tenant_id, conn) do
    GenServer.call(__MODULE__, {:refresh, tenant_id, conn}, 30_000)
  end

  @doc """
  Clears cache for a tenant.
  """
  @spec clear(String.t()) :: :ok
  def clear(tenant_id) do
    GenServer.call(__MODULE__, {:clear, tenant_id})
  end

  @impl true
  def init(_opts) do
    :ets.new(@ets_table, [:named_table, :set, :public, read_concurrency: true])
    :ets.new(@ets_relationships, [:named_table, :set, :public, read_concurrency: true])
    {:ok, %{}}
  end

  @impl true
  def handle_call({:refresh, tenant_id, conn}, _from, state) do
    result = do_refresh(tenant_id, conn)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:clear, tenant_id}, _from, state) do
    match_spec = {{tenant_id, :_, :_}, :_}
    :ets.match_delete(@ets_table, match_spec)
    :ets.match_delete(@ets_relationships, match_spec)
    {:reply, :ok, state}
  end

  defp do_refresh(tenant_id, conn) do
    with {:ok, tables} <- fetch_tables(conn),
         {:ok, columns} <- fetch_columns(conn),
         {:ok, primary_keys} <- fetch_primary_keys(conn),
         {:ok, foreign_keys} <- fetch_foreign_keys(conn) do
      tables_with_columns = build_tables(tables, columns, primary_keys)
      relationships = build_relationships(foreign_keys, primary_keys)

      Enum.each(tables_with_columns, fn table ->
        :ets.insert(@ets_table, {{tenant_id, table.schema, table.name}, table})

        table_rels =
          Enum.filter(relationships, fn r ->
            r.source_schema == table.schema and r.source_table == table.name
          end)

        :ets.insert(@ets_relationships, {{tenant_id, table.schema, table.name}, table_rels})
      end)

      :ok
    end
  end

  defp fetch_tables(conn) do
    query = """
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      c.relkind AS kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')  -- tables, views, materialized views, foreign tables, partitioned
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND n.nspname NOT LIKE 'pg_temp_%'
      AND c.relispartition = false  -- Exclude partition children, keep only logical tables
    ORDER BY n.nspname, c.relname
    """

    case Postgrex.query(conn, query, []) do
      {:ok, %{rows: rows}} ->
        tables =
          Enum.map(rows, fn [schema, name, kind] ->
            %{schema: schema, name: name, is_view: kind in ["v", "m"]}
          end)

        {:ok, tables}

      {:error, _} = error ->
        error
    end
  end

  defp fetch_columns(conn) do
    query = """
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      a.attname AS column_name,
      pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
      NOT a.attnotnull AS is_nullable,
      a.atthasdef AS has_default,
      a.attnum AS position
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
      AND c.relispartition = false
    ORDER BY n.nspname, c.relname, a.attnum
    """

    case Postgrex.query(conn, query, []) do
      {:ok, %{rows: rows}} ->
        columns =
          Enum.map(rows, fn [schema, table, name, type, nullable, has_default, position] ->
            %{
              schema: schema,
              table: table,
              column: %Column{
                name: name,
                type: type,
                nullable: nullable,
                has_default: has_default,
                position: position
              }
            }
          end)

        {:ok, columns}

      {:error, _} = error ->
        error
    end
  end

  defp fetch_primary_keys(conn) do
    query = """
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      con.conname AS constraint_name,
      array_agg(a.attname ORDER BY array_position(con.conkey, a.attnum)) AS columns
    FROM pg_constraint con
    JOIN pg_class c ON c.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
    WHERE con.contype IN ('p', 'u')  -- Primary and unique constraints
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    GROUP BY n.nspname, c.relname, con.conname, con.contype
    ORDER BY n.nspname, c.relname, con.contype DESC  -- Primary keys first
    """

    case Postgrex.query(conn, query, []) do
      {:ok, %{rows: rows}} ->
        pks =
          Enum.map(rows, fn [schema, table, constraint, columns] ->
            %{schema: schema, table: table, constraint: constraint, columns: columns}
          end)

        {:ok, pks}

      {:error, _} = error ->
        error
    end
  end

  defp fetch_foreign_keys(conn) do
    query = """
    SELECT
      con.conname AS constraint_name,
      sn.nspname AS source_schema,
      sc.relname AS source_table,
      array_agg(sa.attname ORDER BY array_position(con.conkey, sa.attnum)) AS source_columns,
      tn.nspname AS target_schema,
      tc.relname AS target_table,
      array_agg(ta.attname ORDER BY array_position(con.confkey, ta.attnum)) AS target_columns
    FROM pg_constraint con
    JOIN pg_class sc ON sc.oid = con.conrelid
    JOIN pg_namespace sn ON sn.oid = sc.relnamespace
    JOIN pg_class tc ON tc.oid = con.confrelid
    JOIN pg_namespace tn ON tn.oid = tc.relnamespace
    JOIN pg_attribute sa ON sa.attrelid = sc.oid AND sa.attnum = ANY(con.conkey)
    JOIN pg_attribute ta ON ta.attrelid = tc.oid AND ta.attnum = ANY(con.confkey)
    WHERE con.contype = 'f'  -- Foreign key constraints
      AND sn.nspname NOT IN ('pg_catalog', 'information_schema')
      AND sc.relispartition = false  -- Exclude partition FKs
    GROUP BY con.conname, sn.nspname, sc.relname, tn.nspname, tc.relname
    ORDER BY sn.nspname, sc.relname, con.conname
    """

    case Postgrex.query(conn, query, []) do
      {:ok, %{rows: rows}} ->
        fks =
          Enum.map(rows, fn [
                              constraint,
                              src_schema,
                              src_table,
                              src_cols,
                              tgt_schema,
                              tgt_table,
                              tgt_cols
                            ] ->
            %{
              constraint: constraint,
              source_schema: src_schema,
              source_table: src_table,
              source_columns: src_cols,
              target_schema: tgt_schema,
              target_table: tgt_table,
              target_columns: tgt_cols
            }
          end)

        {:ok, fks}

      {:error, _} = error ->
        error
    end
  end

  defp build_tables(tables, columns, primary_keys) do
    columns_by_table =
      Enum.group_by(columns, fn c -> {c.schema, c.table} end, fn c -> c.column end)

    pk_by_table =
      primary_keys
      |> Enum.group_by(fn pk -> {pk.schema, pk.table} end)
      |> Enum.map(fn {key, pks} ->
        primary =
          Enum.find(pks, fn pk -> String.ends_with?(pk.constraint, "_pkey") end) || hd(pks)

        {key, primary.columns}
      end)
      |> Map.new()

    Enum.map(tables, fn t ->
      key = {t.schema, t.name}

      %Table{
        schema: t.schema,
        name: t.name,
        columns: Map.get(columns_by_table, key, []),
        primary_key: Map.get(pk_by_table, key, []),
        is_view: t.is_view
      }
    end)
  end

  defp build_relationships(foreign_keys, primary_keys) do
    pk_columns_set =
      primary_keys
      |> Enum.map(fn pk -> {{pk.schema, pk.table}, MapSet.new(pk.columns)} end)
      |> Enum.group_by(fn {key, _} -> key end, fn {_, cols} -> cols end)
      |> Enum.map(fn {key, sets} -> {key, Enum.reduce(sets, MapSet.new(), &MapSet.union/2)} end)
      |> Map.new()

    m2o_relationships =
      Enum.map(foreign_keys, fn fk ->
        source_key = {fk.source_schema, fk.source_table}
        source_pk_cols = Map.get(pk_columns_set, source_key, MapSet.new())
        fk_cols_set = MapSet.new(fk.source_columns)
        is_o2o = MapSet.subset?(fk_cols_set, source_pk_cols)

        %Relationship{
          constraint_name: fk.constraint,
          source_schema: fk.source_schema,
          source_table: fk.source_table,
          source_columns: fk.source_columns,
          target_schema: fk.target_schema,
          target_table: fk.target_table,
          target_columns: fk.target_columns,
          cardinality: if(is_o2o, do: :o2o, else: :m2o),
          junction: nil
        }
      end)

    o2m_relationships =
      Enum.map(m2o_relationships, fn rel ->
        %Relationship{
          constraint_name: rel.constraint_name,
          source_schema: rel.target_schema,
          source_table: rel.target_table,
          source_columns: rel.target_columns,
          target_schema: rel.source_schema,
          target_table: rel.source_table,
          target_columns: rel.source_columns,
          cardinality: if(rel.cardinality == :o2o, do: :o2o, else: :o2m),
          junction: nil
        }
      end)

    m2m_relationships = detect_m2m_relationships(foreign_keys, pk_columns_set)

    m2o_relationships ++ o2m_relationships ++ m2m_relationships
  end

  defp detect_m2m_relationships(foreign_keys, pk_columns_set) do
    fks_by_table =
      Enum.group_by(foreign_keys, fn fk -> {fk.source_schema, fk.source_table} end)

    fks_by_table
    |> Enum.filter(fn {_key, fks} -> length(fks) >= 2 end)
    |> Enum.flat_map(fn {{schema, junction_table}, fks} ->
      junction_pk = Map.get(pk_columns_set, {schema, junction_table}, MapSet.new())

      fks
      |> all_pairs()
      |> Enum.filter(fn {fk1, fk2} ->
        fk1_cols = MapSet.new(fk1.source_columns)
        fk2_cols = MapSet.new(fk2.source_columns)
        combined = MapSet.union(fk1_cols, fk2_cols)
        MapSet.subset?(junction_pk, combined)
      end)
      |> Enum.flat_map(fn {fk1, fk2} ->
        [
          %Relationship{
            constraint_name: "#{fk1.constraint}_#{fk2.constraint}",
            source_schema: fk1.target_schema,
            source_table: fk1.target_table,
            source_columns: fk1.target_columns,
            target_schema: fk2.target_schema,
            target_table: fk2.target_table,
            target_columns: fk2.target_columns,
            cardinality: :m2m,
            junction: %{
              schema: schema,
              table: junction_table,
              source_constraint: fk1.constraint,
              source_columns: fk1.source_columns,
              target_constraint: fk2.constraint,
              target_columns: fk2.source_columns
            }
          },
          %Relationship{
            constraint_name: "#{fk2.constraint}_#{fk1.constraint}",
            source_schema: fk2.target_schema,
            source_table: fk2.target_table,
            source_columns: fk2.target_columns,
            target_schema: fk1.target_schema,
            target_table: fk1.target_table,
            target_columns: fk1.target_columns,
            cardinality: :m2m,
            junction: %{
              schema: schema,
              table: junction_table,
              source_constraint: fk2.constraint,
              source_columns: fk2.source_columns,
              target_constraint: fk1.constraint,
              target_columns: fk1.source_columns
            }
          }
        ]
      end)
    end)
  end

  defp all_pairs([]), do: []
  defp all_pairs([_]), do: []

  defp all_pairs([h | t]) do
    Enum.map(t, fn x -> {h, x} end) ++ all_pairs(t)
  end
end
