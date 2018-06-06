defmodule Mysqlx.MixProject do
  use Mix.Project

  def project do
    [
      app: :mysqlx,
      version: "0.1.0",
      elixir: "~> 1.4",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.0", only: :test},
      {:credo, "~> 0.9.1", only: [:dev, :test], runtime: false},
      {:decimal, "~> 1.0"},
      {:db_connection, "~> 1.1",
       github: "elixir-ecto/db_connection", ref: "4947966"},
      {:connection, "~> 1.0"}
    ]
  end
end
