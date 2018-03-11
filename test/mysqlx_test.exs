defmodule MysqlxTest do
  use ExUnit.Case
  doctest Mysqlx

  defmodule CoderTest do
    use ExUnit.Case, async: true
    import Mysqlx.Messages, only: [auth_plugin_data_2: 1]

    test "auth_plugin_data_2 understands null-terminated strings longer than 12 bytes" do
      ten_bytes = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      auth_plugin_data_2 = "12345678901234"
      obs = auth_plugin_data_2(<<23>> <> ten_bytes <> auth_plugin_data_2 <> <<0>>)
      assert({auth_plugin_data_2, <<>>} == obs)
    end

    test "auth_plugin_data_2 understands null-terminated strings equal to 12 bytes" do
      ten_bytes = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      auth_plugin_data_2 = "123456789012"
      obs = auth_plugin_data_2(<<20>> <> ten_bytes <> auth_plugin_data_2 <> <<0>>)
      assert({auth_plugin_data_2, <<>>} == obs)
    end

    test "auth_plugin_data_2 understands null-terminated strings shorter than 12 bytes" do
      ten_bytes = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      auth_plugin_data_2 = "1234567890"
      obs = auth_plugin_data_2(<<18>> <> ten_bytes <> auth_plugin_data_2 <> <<0, 0, 0>>)
      assert({auth_plugin_data_2, <<>>} == obs)
    end

    test "auth_plugin_data_2 understands fixlen strings longer than 13 bytes" do
      ten_bytes = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      auth_plugin_data_2 = "123456789012345"
      obs = auth_plugin_data_2(<<23>> <> ten_bytes <> auth_plugin_data_2)
      assert({auth_plugin_data_2, <<>>} == obs)
    end

    test "auth_plugin_data_2 understands fixlen strings equal to 13 bytes" do
      ten_bytes = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      auth_plugin_data_2 = "1234567890123"
      obs = auth_plugin_data_2(<<20>> <> ten_bytes <> auth_plugin_data_2)
      assert({auth_plugin_data_2, <<>>} == obs)
    end

    test "auth_plugin_data_2 understands fixlen strings shorter than 13 bytes" do
      ten_bytes = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10>>
      auth_plugin_data_2 = "1234567890"
      obs = auth_plugin_data_2(<<18>> <> ten_bytes <> auth_plugin_data_2 <> <<0, 0, 0>>)
      assert({auth_plugin_data_2, <<>>} == obs)
    end
  end
end
