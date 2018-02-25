defmodule Mysqlx.Query do
  @type t :: %__MODULE__{}
  defstruct type: nil, statement: ""
end
