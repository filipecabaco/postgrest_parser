defmodule PostgrestParser.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      PostgrestParser.SchemaCache
    ]

    opts = [strategy: :one_for_one, name: PostgrestParser.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
