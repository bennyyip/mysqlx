defmodule StartTest do
  use ExUnit.Case, async: true

  @opts [
    database: "mysqlx_test",
    username: "mysqlx_user",
    password: "mysqlx_pass",
    cache_size: 2,
    backoff_type: :stop
  ]

  test "hostname port connection" do
    parent = self()

    test_opts =
      @opts ++
        [
          hostname: "::1",
          after_connect: fn _ -> send(parent, :hi) end
        ]

    assert {:ok, _} = Mysqlx.start_link(test_opts)
    assert_receive :hi
  end

  test "unix domain socket connection" do
    parent = self()

    test_opts =
      @opts ++
        [
          socket: System.get_env("MDBSOCKET") || "/run/mysqld/mysqld.sock",
          after_connect: fn _ -> send(parent, :hi) end
        ]

    assert {:ok, _} = Mysqlx.start_link(test_opts)
    assert_receive :hi
  end

  test "ssl connection" do
    parent = self()

    test_opts = [
      username: "mysqlx_test",
      password: "mysqlx_test_password",
      database: "test",
      ssl: true,
      ssl_opts: [
        cacertfile: "/etc/mysql/ssl/ca-cert.pem",
        keyfile: "/etc/mysql/ssl/client-key.pem",
        certfile: "/etc/mysql/ssl/client-cert.pem"
      ],
      after_connect: fn _ -> send(parent, :hi) end
    ]

    assert {:ok, _} = Mysqlx.start_link(test_opts)
    # short timeout occasionally cause test failed
    assert_receive :hi, 5000
  end
end
