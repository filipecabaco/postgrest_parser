defmodule PostgrestParser.TestConnection do
  @moduledoc """
  Helpers for test database connections.
  """

  @doc """
  Returns connection options for the test database.
  Uses environment variables or defaults to docker-compose settings.
  """
  def connection_opts do
    [
      hostname: System.get_env("POSTGRES_HOST", "localhost"),
      port: String.to_integer(System.get_env("POSTGRES_PORT", "5433")),
      username: System.get_env("POSTGRES_USER", "postgres"),
      password: System.get_env("POSTGRES_PASSWORD", "postgres"),
      database: System.get_env("POSTGRES_DB", "postgrest_parser_test")
    ]
  end

  @doc """
  Starts a connection to the test database.
  """
  def start_connection do
    Postgrex.start_link(connection_opts())
  end
end
