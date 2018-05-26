defmodule Mysqlx.Query do
  @moduledoc false
  @type t :: %__MODULE__{type: nil, statement: String, ref: nil}
  defstruct type: nil, statement: "", ref: nil
end

defimpl DBConnection.Query, for: Mysqlx.Query do
  alias Mysqlx.Query

  def encode(%Mysqlx.Query{type: :text}, [], _opts) do
    []
  end

  def decode(query, result, opts) do
  end

  def describe(query, _opts) do
    query
  end

  def parse(%{name: name, statement: statement, ref: nil} = query, _opts) do
    %{
      query
      | name: IO.iodata_to_binary(name),
        statement: IO.iodata_to_binary(statement)
    }
  end
end
