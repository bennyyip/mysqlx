defmodule Mysqlx.Query do
  @moduledoc false
  @type t :: %__MODULE__{}
  defstruct type: nil, statement: ""
end
