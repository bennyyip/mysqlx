defmodule Mysqlx do
  @moduledoc """
  Documentation for Mysqlx.
  """
  alias Mysqlx.Query

  def run() do
    {:ok, pid} =
      start_link(
        username: "mysqlx_user",
        password: "mysqlx_pass",
        database: "mysqlx_test",
        hostname: "::1"
      )

    Mysqlx.query(pid, "select * from test_pass ", [])
  end

  def start_link(opts) do
    DBConnection.start_link(Mysqlx.Protocol, opts)
  end

  def query(conn, statement, params \\ [], opts \\ []) do
    DBConnection.execute(
      conn,
      %Query{type: :text, statement: statement},
      params,
      opts
    )
  end
end
