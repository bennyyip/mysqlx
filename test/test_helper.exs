mdb_socket = System.get_env("MDBSOCKET") || "/run/mysqld/mysqld.sock"
otp_release = :otp_release |> :erlang.system_info() |> List.to_integer()

unix_exclude = [
  unix:
    not (otp_release >= 20 and is_binary(mdb_socket) and
           File.exists?(mdb_socket))
]

json_exclude = [json: System.get_env("JSON_SUPPORT") != "true"]
geometry_exclude = [geometry: System.get_env("GEOMETRY_SUPPORT") == "false"]

ExUnit.start(exclude: unix_exclude ++ json_exclude ++ geometry_exclude)

run_cmd = fn cmd ->
  key = :ecto_setup_cmd_output
  Process.put(key, "")

  status =
    Mix.Shell.cmd(cmd, fn data ->
      current = Process.get(key)
      Process.put(key, current <> data)
    end)

  output = Process.get(key)
  Process.put(key, "")
  {status, output}
end

mysql_pass_switch =
  if mysql_root_pass = System.get_env("MYSQL_ROOT_PASSWORD") do
    "-p#{mysql_root_pass}"
  else
    ""
  end

mysql_port = System.get_env("MDBPORT") || 3306
mysql_host = System.get_env("MDBHOST") || "localhost"

mysql_connect =
  "-u root #{mysql_pass_switch} --host=#{mysql_host} --port=#{mysql_port} --protocol=tcp"

sql = """
  CREATE TABLE test1 (id serial, title text);
  INSERT INTO test1 VALUES(1, 'test');
  INSERT INTO test1 VALUES(2, 'test2');
  DROP TABLE test1;
"""

create_user =
  case System.get_env("DB") do
    "mysql:5.6" -> "CREATE USER"
    _ -> "CREATE USER IF NOT EXISTS"
  end

cmds = [
  ~s(mysql #{mysql_connect} -e "DROP DATABASE IF EXISTS mysqlx_test;"),
  ~s(mysql #{mysql_connect} -e "CREATE DATABASE mysqlx_test DEFAULT CHARACTER SET 'utf8' COLLATE 'utf8_general_ci';"),
  ~s(mysql #{mysql_connect} -e "#{create_user} 'mysqlx_user'@'%' IDENTIFIED BY 'mysqlx_pass';"),
  ~s(mysql #{mysql_connect} -e "GRANT ALL ON *.* TO 'mysqlx_user'@'%' WITH GRANT OPTION"),
  ~s(mysql --host=#{mysql_host} --port=#{mysql_port} --protocol=tcp -u mysqlx_user -pmysqlx_pass mysqlx_test -e "#{
    sql
  }")
]

Enum.each(cmds, fn cmd ->
  {status, output} = run_cmd.(cmd)
  IO.puts("--> #{output}")

  if status != 0 do
    IO.puts("""
    Command:
    #{cmd}
    error'd with:
    #{output}
    Please verify the user "root" exists and it has permissions to
    create databases and users.
    If the "root" user requires a password, set the environment
    variable MYSQL_ROOT_PASSWORD to its value.
    Beware that the password may be visible in the process list!
    """)

    System.halt(1)
  end
end)

defmodule Mysqlx.TestHelper do
  defmacro query(stat, params, opts \\ []) do
    quote do
      pid = var!(context)[:pid]

      case Mysqlx.query(
             pid,
             unquote(stat),
             unquote(params),
             unquote(opts)
           ) do
        {:ok, %Mysqlx.Result{rows: nil}} -> :ok
        {:ok, %Mysqlx.Result{rows: rows}} -> rows
        {:error, %Mysqlx.Error{} = err} -> err
      end
    end
  end

  defmacro execute_text(stat, params, opts \\ []) do
    quote do
      opts = [query_type: :text] ++ unquote(opts)

      case Mysqlx.query(
             var!(context)[:pid],
             unquote(stat),
             unquote(params),
             opts
           ) do
        {:ok, %Mysqlx.Result{rows: nil}} -> :ok
        {:ok, %Mysqlx.Result{rows: rows}} -> rows
        {:error, %Mysqlx.Error{} = err} -> err
      end
    end
  end

  defmacro with_prepare!(name, stat, params, opts \\ []) do
    quote do
      conn = var!(context)[:pid]

      query = Mysqlx.prepare!(conn, unquote(name), unquote(stat), unquote(opts))

      case Mysqlx.execute!(conn, query, unquote(params)) do
        %Mysqlx.Result{rows: nil} -> :ok
        %Mysqlx.Result{rows: rows} -> rows
      end
    end
  end

  defmacro prepare(stat, opts \\ []) do
    quote do
      case Mysqlx.prepare(var!(context)[:pid], unquote(stat), unquote(opts)) do
        {:ok, %Mysqlx.Query{} = query} -> query
        {:error, %Mysqlx.Error{} = err} -> err
      end
    end
  end

  defmacro execute(query, params, opts \\ []) do
    quote do
      case Mysqlx.execute(
             var!(context)[:pid],
             unquote(query),
             unquote(params),
             unquote(opts)
           ) do
        {:ok, %Mysqlx.Result{rows: nil}} -> :ok
        {:ok, %Mysqlx.Result{rows: rows}} -> rows
        {:error, %Mysqlx.Error{} = err} -> err
      end
    end
  end

  defmacro close(query, opts \\ []) do
    quote do
      case Mysqlx.close(var!(context)[:pid], unquote(query), unquote(opts)) do
        :ok -> :ok
        {:error, %Mysqlx.Error{} = err} -> err
      end
    end
  end

  def length_encode_row(row) do
    Enum.map_join(row, &(<<String.length(&1)>> <> &1))
  end
end
