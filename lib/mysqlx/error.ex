defmodule Mysqlx.Error do
  @moduledoc false
  defexception [:message, :mariadb, :connection_id]

  @type t :: %Mysqlx.Error{}

  def message(e) do
    e.message || ""
  end

  def exception(arg) do
    super(arg)
  end
end
