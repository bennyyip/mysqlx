defmodule Mysqlx.Error do
  defexception [:message, :tag, :action, :reason, :mysql, :connection_id]

  def message(e) do
    cond do
      kw = e.mysql ->
        "(#{kw[:code]}): #{kw[:message]}"

      tag = e.tag ->
        "[#{tag}] `#{e.action}` failed with: #{inspect(e.reason)}"

      true ->
        e.message || ""
    end
  end
end
