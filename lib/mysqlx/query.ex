defmodule Mysqlx.Query do
  @moduledoc false
  @type t :: %__MODULE__{type: nil, statement: String, ref: nil}
  defstruct type: nil, statement: "", ref: nil
end

defimpl DBConnection.Query, for: Mysqlx.Query do
  alias Mysqlx.Query
  alias Mysqlx.Column

  def encode(%Mysqlx.Query{type: :text}, [], _opts) do
    []
  end

  def decode(_, {res, nil}, _) do
    %Mysqlx.Result{res | rows: nil}
  end

  def decode(_, {res, columns}, opts) do
    %Mysqlx.Result{rows: rows} = res
    decoded = do_decode(rows, opts)
    include_table_name = opts[:include_table_name]

    columns =
      for %Column{} = column <- columns,
          do: column_name(column, include_table_name)

    %Mysqlx.Result{
      res
      | rows: decoded,
        columns: columns,
        num_rows: length(decoded)
    }
  end

  defp column_name(%Column{name: name, table: table}, true),
    do: "#{table}.#{name}"

  defp column_name(%Column{name: name}, _), do: name

  defp do_decode(rows, opts) do
    case Keyword.get(opts, :decode_mapper) do
      nil ->
        Enum.reverse(rows)

      mapper when is_function(mapper, 1) ->
        do_decode(rows, mapper, [])
    end
  end

  defp do_decode([row | rows], mapper, acc) do
    do_decode(rows, mapper, [mapper.(row) | acc])
  end

  defp do_decode([], _, acc) do
    acc
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
