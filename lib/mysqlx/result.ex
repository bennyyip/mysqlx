defmodule Mysqlx.Result do
  @moduledoc false
  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          rows: [tuple] | nil,
          last_insert_id: integer,
          num_rows: integer,
          connection_id: nil
        }
  defstruct [:columns, :rows, :last_insert_id, :num_rows, :connection_id]
end
