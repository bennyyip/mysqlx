defmodule Mysqlx do
  @moduledoc """
  Documentation for Mysqlx.
  """

  def test do
    {:ok, _pid} =
      start_link(
        username: "mysqlx_test",
        password: "mysqlx_test_password",
        database: "test"
      )
  end

  def start_link(opts) do
    DBConnection.start_link(Mysqlx.Protocol, opts)
  end
end
