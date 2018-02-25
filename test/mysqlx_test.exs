defmodule MysqlxTest do
  use ExUnit.Case
  doctest Mysqlx

  test "greets the world" do
    assert Mysqlx.hello() == :world
  end
end
