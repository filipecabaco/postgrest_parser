defmodule PostgrestParser.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :postgrest_parser,
      version: @version,
      elixir: "~> 1.14",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      elixirc_paths: elixirc_paths(Mix.env()),
      description: description(),
      package: package(),
      docs: docs(),
      name: "PostgrestParser",
      source_url: "https://github.com/supabase/postgrest_parser",
      dialyzer: dialyzer(),
      aliases: aliases(),
      test_coverage: [summary: [threshold: 91]]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {PostgrestParser.Application, []}
    ]
  end

  defp deps do
    [
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.4"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:sobelow, "~> 0.13", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "PostgREST URL-to-SQL parser for Elixir. Parse PostgREST-style query strings into parameterized SQL queries."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/supabase/postgrest_parser"}
    ]
  end

  defp docs do
    [
      main: "PostgrestParser",
      extras: ["README.md"]
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      plt_add_apps: [:ex_unit]
    ]
  end

  defp aliases do
    [
      quality: ["format", "credo --strict", "sobelow --config", "dialyzer"],
      "quality.ci": [
        "format --check-formatted",
        "credo --strict",
        "sobelow --exit --config",
        "dialyzer"
      ]
    ]
  end
end
