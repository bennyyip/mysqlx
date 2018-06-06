defmodule Mysqlx do
  @moduledoc """
  Documentation for Mysqlx.
  """
  alias Mysqlx.Query

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
