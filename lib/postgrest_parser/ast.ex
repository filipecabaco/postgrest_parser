defmodule PostgrestParser.AST do
  @moduledoc """
  Abstract Syntax Tree structures for parsed PostgREST URL parameters.

  These structs represent the intermediate form between URL query strings
  and executable SQL queries.
  """

  defmodule Field do
    @moduledoc """
    Represents a field reference, optionally with JSON path operations and type casting.
    """
    @type json_op ::
            {:arrow, String.t()} | {:double_arrow, String.t()} | {:array_index, integer()}

    @type t :: %__MODULE__{
            name: String.t(),
            json_path: [json_op()],
            cast: String.t() | nil
          }

    defstruct [:name, :cast, json_path: []]
  end

  defmodule Filter do
    @moduledoc """
    Represents a filter condition: field.operator.value

    Examples:
    - id=eq.1 -> %Filter{field: "id", operator: :eq, value: "1"}
    - name=not.eq.john -> %Filter{field: "name", operator: :eq, value: "john", negated?: true}
    - name=like(any).{John%,Jane%} -> %Filter{field: "name", operator: :like, value: ["John%", "Jane%"], quantifier: :any}
    """
    @type operator ::
            :eq
            | :neq
            | :gt
            | :gte
            | :lt
            | :lte
            | :like
            | :ilike
            | :match
            | :imatch
            | :in
            | :is
            | :fts
            | :plfts
            | :phfts
            | :wfts
            | :cs
            | :cd
            | :ov
            | :sl
            | :sr
            | :nxl
            | :nxr
            | :adj

    @type quantifier :: :any | :all

    @type t :: %__MODULE__{
            field: Field.t() | String.t(),
            operator: operator(),
            value: String.t() | [String.t()],
            quantifier: quantifier() | nil,
            language: String.t() | nil,
            negated?: boolean()
          }

    defstruct [:field, :operator, :value, :quantifier, :language, negated?: false]
  end

  defmodule SelectItem do
    @moduledoc """
    Represents a select item which can be a field, relation, or spread.

    Examples:
    - name -> %SelectItem{type: :field, name: "name"}
    - alias:name -> %SelectItem{type: :field, name: "name", alias: "alias"}
    - client(id,name) -> %SelectItem{type: :relation, name: "client", children: [...]}
    - ...client(id) -> %SelectItem{type: :spread, name: "client", children: [...]}
    """
    @type item_type :: :field | :relation | :spread

    @type t :: %__MODULE__{
            type: item_type(),
            name: String.t(),
            alias: String.t() | nil,
            children: [t()] | nil,
            hint: String.t() | nil
          }

    defstruct [:type, :name, :alias, :children, :hint]
  end

  defmodule OrderTerm do
    @moduledoc """
    Represents an order clause: field.direction.nulls

    Examples:
    - id -> %OrderTerm{field: "id", direction: :asc}
    - id.desc -> %OrderTerm{field: "id", direction: :desc}
    - id.desc.nullsfirst -> %OrderTerm{field: "id", direction: :desc, nulls: :first}
    """
    @type direction :: :asc | :desc
    @type nulls :: :first | :last | nil

    @type t :: %__MODULE__{
            field: Field.t() | String.t(),
            direction: direction(),
            nulls: nulls()
          }

    defstruct [:field, direction: :asc, nulls: nil]
  end

  defmodule LogicTree do
    @moduledoc """
    Represents boolean logic trees: and(...), or(...), not.and(...)

    Examples:
    - and(id.eq.1,name.eq.john) -> LogicTree with :and operator
    - or(id.eq.1,id.eq.2) -> LogicTree with :or operator
    - not.and(...) -> LogicTree with negated?: true
    """
    @type logic_operator :: :and | :or

    @type t :: %__MODULE__{
            operator: logic_operator(),
            conditions: [Filter.t() | t()],
            negated?: boolean()
          }

    defstruct [:operator, :conditions, negated?: false]
  end

  defmodule ParsedParams do
    @moduledoc """
    The complete result of parsing a PostgREST query string.
    """
    @type t :: %__MODULE__{
            select: [SelectItem.t()] | nil,
            filters: [Filter.t() | LogicTree.t()],
            order: [OrderTerm.t()],
            limit: non_neg_integer() | nil,
            offset: non_neg_integer() | nil
          }

    defstruct select: nil, filters: [], order: [], limit: nil, offset: nil
  end
end
