defmodule Mysqlx do
  @moduledoc """
  Documentation for Mysqlx.
  """

  def start_link(opts) do
    DBConnection.start_link(Mysqlx.Protocol, opts)
  end
end
