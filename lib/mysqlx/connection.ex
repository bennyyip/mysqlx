defmodule Mysqlx.Connection do
  @moduledoc """
  Main API for Mysqlx. This module handles the connection to .
  """

  defdelegate start_link(opts), to: Mysqlx
  defdelegate query(conn, statement), to: Mysqlx
  defdelegate query(conn, statement, params), to: Mysqlx
  defdelegate query(conn, statement, params, opts), to: Mysqlx
  defdelegate query!(conn, statement), to: Mysqlx
  defdelegate query!(conn, statement, params), to: Mysqlx
  defdelegate query!(conn, statement, params, opts), to: Mysqlx
end
