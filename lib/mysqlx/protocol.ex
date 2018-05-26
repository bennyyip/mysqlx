defmodule Mysqlx.Protocol do
  @moduledoc false
  require Logger
  use DBConnection
  use Bitwise

  alias DBConnection.ConnectionError, as: DBConnectionError
  import Mysqlx.Messages
  import Mysqlx.ProtocolHelper
  alias Mysqlx.Query
  alias Mysqlx.Column

  @maxpacketbytes 50_000_000
  @mysql_native_password "mysql_native_password"
  @mysql_old_password :mysql_old_password

  @timeout 15_000
  @nonposix_errors [:closed, :timeout]
  @sock_opts [packet: :raw, mode: :binary, active: false]
  @cache_size 100

  @client_long_password 0x00000001
  @client_found_rows 0x00000002
  @client_long_flag 0x00000004
  @client_connect_with_db 0x00000008
  @client_local_files 0x00000080
  @client_protocol_41 0x00000200
  @client_ssl 0x00000800
  @client_transactions 0x00002000
  @client_secure_connection 0x00008000
  @client_multi_statements 0x00010000
  @client_multi_results 0x00020000
  @client_ps_multi_results 0x00040000
  @client_deprecate_eof 0x01000000

  @server_more_results_exists 0x0008
  @server_status_cursor_exists 0x0040
  @server_status_last_row_sent 0x0080

  @cursor_type_no_cursor 0x00
  @cursor_type_read_only 0x01

  @capabilities @client_long_password ||| @client_found_rows |||
                  @client_long_flag ||| @client_local_files |||
                  @client_protocol_41 ||| @client_transactions |||
                  @client_secure_connection ||| @client_multi_statements |||
                  @client_multi_results ||| @client_ps_multi_results |||
                  @client_deprecate_eof

  defstruct sock: nil,
            timeout: nil,
            buffer: "",
            # :handshake | :handshake_send
            state: nil,
            state_data: nil,
            deprecated_eof: true,
            connection_id: nil,
            #  :undefined | :not_used | :ssl_handshake | :connected
            ssl_conn_state: :undefined

  @type state :: %__MODULE__{}

  defp default_opts(opts) do
    opts
    |> Keyword.put_new(
      :username,
      System.get_env("MDBUSER") || System.get_env("USER")
    )
    |> Keyword.put_new(:password, System.get_env("MDBPASSWORD"))
    |> Keyword.put_new(:hostname, System.get_env("MDBHOST") || "localhost")
    |> Keyword.put_new(:port, System.get_env("MDBPORT") || 3306)
    |> Keyword.put_new(:timeout, @timeout)
    |> Keyword.put_new(:cache_size, @cache_size)
    |> Keyword.put_new(:sock_type, :tcp)
    |> Keyword.put_new(:socket_options, [])
    |> Keyword.update!(:port, &normalize_port/1)
  end

  # DB Connection callbacks
  def checkout(%{buffer: :active_once} = s) do
    case setopts(s, [active: false], :active_once) do
      :ok -> recv_buffer(s)
      {:disconnect, _, _} = dis -> dis
    end
  end

  def checkin(%{buffer: buffer} = s) when is_binary(buffer) do
    activate(s, buffer)
  end

  @spec connect(Keyword.t()) ::
          {:ok, state}
          | {:error, Mysqlx.Error.t() | %DBConnection.ConnectionError{}}
  def connect(opts) do
    opts = default_opts(opts)

    {host, port} =
      case Keyword.fetch(opts, :socket) do
        # unix domain socket
        {:ok, socket} ->
          {{:local, socket}, 0}

        # hostname:port
        :error ->
          {parse_host(opts[:hostname]), opts[:port]}
      end

    timeout = opts[:timeout] || @timeout
    sock_opts = [send_timeout: timeout] ++ (opts[:socket_options] || [])

    s = %__MODULE__{
      timeout: timeout,
      state: :handshake,
      ssl_conn_state: set_initial_ssl_conn_state(opts),
      connection_id: self()
    }

    case connect(host, port, sock_opts, timeout, s) do
      {:ok, s} -> handshake_recv(s, %{opts: opts})
      {:error, _} = error -> error
    end
  end

  defp connect(host, port, sock_opts, timeout, s) do
    buffer? = Keyword.has_key?(sock_opts, :buffer)

    case :gen_tcp.connect(host, port, sock_opts ++ @sock_opts, timeout) do
      {:ok, sock} when buffer? ->
        {:ok, %{s | sock: {:gen_tcp, sock}}}

      {:ok, sock} ->
        # A suitable :buffer is only set if :recbuf is included in
        # :socket_options.
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(sock, [:sndbuf, :recbuf, :buffer])

        buffer =
          buffer
          |> max(sndbuf)
          |> max(recbuf)

        :ok = :inet.setopts(sock, buffer: buffer)
        {:ok, %{s | sock: {:gen_tcp, sock}}}

      {:error, reason} ->
        case host do
          {:local, socket_addr} ->
            {:error, conn_error(:tcp, "connect (#{socket_addr})", reason)}

          host ->
            {:error, conn_error(:tcp, "connect (#{host}:#{port})", reason)}
        end
    end
  end

  @doc """
  DBConnection callback
  """
  def disconnect(_, state = %{sock: {sock_mod, sock}}) do
    msg_send(msg_text_cmd(command: com_quit(), statement: ""), state, 0)

    case msg_recv(state) do
      {:ok, packet(msg: msg_ok()), _state} ->
        sock_mod.close(sock)

      {:ok, packet(msg: _), _state} ->
        sock_mod.close(sock)

      {:error, _} ->
        sock_mod.close(sock)
    end

    _ = recv_buffer(state)
    :ok
  end

  defp recv_error(reason, %{sock: {sock_mod, _}} = state) do
    do_disconnect(state, {tag(sock_mod), "recv", reason, ""})
  end

  def do_disconnect(s, {tag, action, reason, buffer}) do
    err = Mysqlx.Error.exception(tag: tag, action: action, reason: reason)
    do_disconnect(s, err, buffer)
  end

  defp do_disconnect(
         %{connection_id: connection_id} = state,
         %Mysqlx.Error{} = err,
         buffer
       ) do
    {:disconnect, %{err | connection_id: connection_id},
     %{state | buffer: buffer}}
  end

  def_handle(:ping_recv, :ping_handle)

  def ping(%{buffer: buffer} = state) when is_binary(buffer) do
    msg_send(msg_text_cmd(command: com_ping(), statement: ""), state, 0)
    ping_recv(state, :ping)
  end

  def ping(state) do
    case checkout(state) do
      {:ok, state} ->
        msg_send(msg_text_cmd(command: com_ping(), statement: ""), state, 0)
        {:ok, state} = ping_recv(state, :ping)
        checkin(state)

      {:disconnect, _, _} = dis ->
        dis
    end
  end

  defp ping_handle(packet(msg: msg_ok()), :ping, %{buffer: buffer} = state)
       when is_binary(buffer) do
    {:ok, state}
  end

  defp send_text_query(s, statement) do
    msg_send(msg_text_cmd(command: com_query(), statement: statement), s, 0)
    %{s | state: :column_count}
  end

  defp text_query_recv(state, query) do
    case text_query_recv(state) do
      {:resultset, columns, rows, _flags, state} ->
        result = %Mysqlx.Result{rows: rows, connection_id: state.connection_id}
        {:ok, {result, columns}, clean_state(state)}

      {:ok, packet(msg: msg_ok()) = packet, state} ->
        handle_ok_packet(packet, query, state)

      {:ok, packet, state} ->
        {:error, "TODO"}

      # handle_error(packet, query, state)

      {:error, reason} ->
        recv_error(reason, state)
    end
  end

  defp text_query_recv(state) do
    state = %{state | state: :column_count}

    with {:ok, packet(msg: msg_column_count(column_count: num_cols)), state} <-
           msg_recv(state),
         {:eof, columns, _, state} <- columns_recv(state, num_cols),
         {:eof, rows, flags, state} <- text_rows_recv(state, columns) do
      {:resultset, columns, rows, flags, state}
    end
  end

  defp text_rows_recv(state, columns) do
    # TODO
    {:eof, 0, 0, state}
  end

  defp columns_recv(state, num_cols) do
    columns_recv(%{state | state: :column_definitions}, num_cols, [])
  end

  defp columns_recv(state, rem, columns) when rem > 0 do
    case msg_recv(state) do
      {:ok,
       packet(
         msg:
           msg_column_definition(
             type: type,
             name: name,
             flags: flags,
             table: table
           )
       ), state} ->
        column = %Column{name: name, table: table, type: type, flags: flags}
        columns_recv(state, rem - 1, [column | columns])

      other ->
        other
    end
  end

  defp columns_recv(%{deprecated_eof: true} = state, 0, columns) do
    {:eof, Enum.reverse(columns), 0, state}
  end

  defp handshake_recv(state, request) do
    case msg_recv(state) do
      {:ok, packet, state} ->
        case handle_handshake(packet, request, state) do
          {:error, error} ->
            # TODO
            Logger.error(inspect(error))

          other ->
            other
        end

      {:error, reason} ->
        Logger.error(inspect(reason))
    end
  end

  # SSL Request
  defp handle_handshake(
         packet(seqnum: seqnum) = packet,
         opts,
         %{ssl_conn_state: :ssl_handshake} = s
       ) do
    # Create and send an SSL request packet per the spec:
    # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
    msg =
      msg_ssl_request(
        client_capabilities: ssl_capabilities(opts),
        max_packet_size: @maxpacketbytes,
        character_set: 8
      )

    new_seqnum = seqnum + 1
    msg_send(msg, s, new_seqnum)

    Logger.info("Sent SSL requelst")

    case upgrade_to_ssl(s, opts) do
      {:ok, new_state} ->
        # move along to the actual handshake; now over SSL/TLS
        Logger.info("SSL connection established")
        handle_handshake(packet(packet, seqnum: new_seqnum), opts, new_state)

      {:error, error} ->
        {:error, error}
    end
  end

  # reply handshake response
  defp handle_handshake(
         packet(
           seqnum: seqnum,
           msg:
             msg_handshake(
               capability_flags_1: flag1,
               capability_flags_2: flag2,
               plugin: plugin
             ) = handshake
         ) = _packet,
         %{opts: opts} = _request,
         s
       ) do
    <<flag::size(32)>> = <<flag2::size(16), flag1::size(16)>>

    deprecated_eof = (flag &&& @client_deprecate_eof) == @client_deprecate_eof

    Logger.info("Got handshake packet")

    msg_handshake(auth_plugin_data_1: salt1, auth_plugin_data_2: salt2) =
      handshake

    scramble =
      case password = opts[:password] do
        nil -> ""
        "" -> ""
        _ -> password(plugin, password, <<salt1::binary, salt2::binary>>)
      end

    {database, capabilities} = capabilities(opts)

    msg =
      msg_handshake_response(
        username: :unicode.characters_to_binary(opts[:username]),
        password: scramble,
        database: database,
        client_capabilities: capabilities,
        max_packet_size: @maxpacketbytes,
        character_set: 8
      )

    msg_send(msg, s, seqnum + 1)
    Logger.info("Sent handshake response packet")

    handshake_recv(
      %{s | state: :handshake_send, deprecated_eof: deprecated_eof},
      nil
    )
  end

  # recieve ok packet
  defp handle_handshake(
         packet(
           msg:
             msg_ok(
               affected_rows: _affected_rows,
               last_insert_id: _last_insert_id
             ) = _packet
         ),
         nil,
         state
       ) do
    Logger.info("Connected to MySQL server")

    activate(state, state.buffer) |> connected()
    # statement = "SET CHARACTER SET " <> (state.opts[:charset] || "utf8")
    # query = %Query{type: :text, statement: statement}
    # case send_text_query(state, statement) |> text_query_recv(query) do
    #   {:error, error, _} ->
    #     {:error, error}
    #   {:ok, _, state} ->
    #     activate(state, state.buffer) |> connected()
    # end
  end

  # recieve error packet
  defp handle_handshake(packet, query, state) do
    # {:error, error, _} = handle_error(packet, query, state)
    Logger.error("Failed to connect: #{inspect(packet)}")
    {:error, %Mysqlx.Error{}}
  end

  defp set_initial_ssl_conn_state(opts) do
    if opts[:ssl] && has_ssl_opts?(opts[:ssl_opts]) do
      :ssl_handshake
    else
      :not_used
    end
  end

  def handle_execute(
        %Query{type: :text, statement: statement} = query,
        [],
        _opts,
        state
      ) do
    send_text_query(state, statement) |> text_query_recv(query)
  end

  defp handle_ok_packet(
         packet(
           msg:
             msg_ok(
               affected_rows: affected_rows,
               last_insert_id: last_insert_id
             )
         ),
         _query,
         s
       ) do
    result = %Mysqlx.Result{
      columns: [],
      rows: nil,
      num_rows: affected_rows,
      last_insert_id: last_insert_id,
      connection_id: s.connection_id
    }

    {:ok, {result, nil}, clean_state(s)}
  end

  defp clean_state(state) do
    %{state | state: :running, state_data: nil}
  end

  defp tag(:gen_tcp), do: :tcp
  defp tag(:ssl), do: :ssl

  defp has_ssl_opts?(nil), do: false
  defp has_ssl_opts?([]), do: false
  defp has_ssl_opts?(ssl_opts) when is_list(ssl_opts), do: true

  defp normalize_port(port) when is_binary(port), do: String.to_integer(port)

  defp normalize_port(port) when is_integer(port), do: port

  defp parse_host(host) do
    host = if is_binary(host), do: String.to_charlist(host), else: host

    case :inet.parse_strict_address(host) do
      {:ok, address} ->
        address

      _ ->
        host
    end
  end

  defp msg_send(msg, %{sock: {sock_mod, sock}}, seqnum),
    do: msg_send(msg, {sock_mod, sock}, seqnum)

  defp msg_send(msgs, {sock_mod, sock}, seqnum) when is_list(msgs) do
    binaries = Enum.reduce(msgs, [], &[&2 | encode(&1, seqnum)])
    sock_mod.send(sock, binaries)
  end

  defp msg_send(msg, {sock_mod, sock}, seqnum) do
    data = encode(msg, seqnum)
    sock_mod.send(sock, data)
  end

  defp msg_recv(%__MODULE__{sock: sock_info, buffer: buffer} = state) do
    msg_recv(sock_info, state, buffer)
  end

  defp msg_recv(sock, state, buffer) do
    case msg_decode(buffer, state) do
      {:ok, _packet, _new_state} = success ->
        success

      {:more, more} ->
        msg_recv(sock, state, buffer, more)

      {:error, _} = err ->
        err
    end
  end

  defp msg_recv({sock_mod, sock} = s, state, buffer, more) do
    case sock_mod.recv(sock, more, state.timeout) do
      {:ok, data} when byte_size(data) < more ->
        msg_recv(s, state, [buffer | data], more - byte_size(data))

      {:ok, data} when is_binary(buffer) ->
        msg_recv(s, state, buffer <> data)

      {:ok, data} when is_list(buffer) ->
        msg_recv(s, state, IO.iodata_to_binary([buffer | data]))

      {:error, _} = err ->
        err
    end
  end

  def msg_decode(
        <<len::size(24)-little-integer, _seqnum::size(8)-integer,
          message::binary>> = header,
        state
      )
      when byte_size(message) >= len do
    {packet, rest} = decode(header, state.state)
    {:ok, packet, %{state | buffer: rest}}
  end

  def msg_decode(_buffer, _state) do
    {:more, 0}
  end

  defp conn_error(mod, action, reason) when reason in @nonposix_errors do
    conn_error("#{mod} #{action}: #{reason}")
  end

  defp conn_error(:tcp, action, reason) do
    formatted_reason = :inet.format_error(reason)
    conn_error("tcp #{action}: #{formatted_reason} - #{inspect(reason)}")
  end

  defp conn_error(:ssl, action, reason) do
    formatted_reason = :ssl.format_error(reason)
    conn_error("ssl #{action}: #{formatted_reason} - #{inspect(reason)}")
  end

  defp conn_error(message) do
    DBConnectionError.exception(message)
  end

  defp connected({:disconnect, error, state}) do
    disconnect(error, state)
    {:error, error}
  end

  defp connected(other), do: other

  defp recv_buffer(%{sock: {:gen_tcp, sock}} = s) do
    receive do
      {:tcp, ^sock, buffer} ->
        {:ok, %{s | buffer: buffer}}

      {:tcp_closed, ^sock} ->
        {:disconnect, {:tcp, "async_recv", :closed, ""}}

      {:tcp_error, ^sock, reason} ->
        {:disconnect, {:tcp, "async_recv", reason, ""}}
    after
      0 ->
        {:ok, %{s | buffer: <<>>}}
    end
  end

  defp recv_buffer(%{sock: {:ssl, sock}} = s) do
    receive do
      {:ssl, ^sock, buffer} ->
        {:ok, %{s | buffer: buffer}}

      {:ssl_closed, ^sock} ->
        {:disconnect, {:ssl, "async_recv", :closed, ""}}

      {:ssl_error, ^sock, reason} ->
        {:disconnect, {:ssl, "async_recv", reason, ""}}
    after
      0 ->
        {:ok, %{s | buffer: <<>>}}
    end
  end

  ## fake [active: once] if buffer not empty
  defp activate(s, <<>>) do
    case setopts(s, [active: :once], <<>>) do
      :ok -> {:ok, %{s | buffer: :active_once}}
      other -> other
    end
  end

  defp activate(%{sock: {mod, sock}} = s, buffer) do
    _ = send(self(), {tag(mod), sock, buffer})
    {:ok, s}
  end

  defp setopts(%{sock: {mod, sock}} = s, opts, buffer) do
    case setopts(mod, sock, opts) do
      :ok ->
        :ok

      {:error, reason} ->
        do_disconnect(s, {tag(mod), "setopts", reason, buffer})
    end
  end

  defp setopts(:gen_tcp, sock, opts), do: :inet.setopts(sock, opts)
  defp setopts(:ssl, sock, opts), do: :ssl.setopts(sock, opts)

  defp upgrade_to_ssl(%{sock: {_sock_mod, sock}} = s, %{opts: opts}) do
    ssl_opts = opts[:ssl_opts]

    case :ssl.connect(sock, ssl_opts, opts[:timeout]) do
      {:ok, ssl_sock} ->
        # switch to the ssl connection module
        # set the socket
        # move ssl_conn_state to :connected
        {:ok, %{s | sock: {:ssl, ssl_sock}, ssl_conn_state: :connected}}

      {:error, reason} ->
        {:error,
         %Mysqlx.Error{message: "failed to upgraded socket: #{inspect(reason)}"}}
    end
  end

  defp capabilities(opts) do
    case opts[:skip_database] do
      true -> {"", @capabilities}
      _ -> {opts[:database], @capabilities ||| @client_connect_with_db}
    end
  end

  defp ssl_capabilities(%{opts: opts}) do
    case opts[:skip_database] do
      true -> @capabilities ||| @client_ssl
      _ -> @capabilities ||| @client_connect_with_db ||| @client_ssl
    end
  end

  defp password(@mysql_native_password <> _, password, salt),
    do: mysql_native_password(password, salt)

  defp password("", password, salt), do: mysql_native_password(password, salt)

  defp password(@mysql_old_password, password, salt),
    do: mysql_old_password(password, salt)

  defp mysql_native_password(password, salt) do
    stage1 = :crypto.hash(:sha, password)
    stage2 = :crypto.hash(:sha, stage1)

    :crypto.hash_init(:sha)
    |> :crypto.hash_update(salt)
    |> :crypto.hash_update(stage2)
    |> :crypto.hash_final()
    |> bxor_binary(stage1)
  end

  defp bxor_binary(b1, b2) do
    for(
      {e1, e2} <-
        List.zip([:erlang.binary_to_list(b1), :erlang.binary_to_list(b2)]),
      do: e1 ^^^ e2
    )
    |> :erlang.list_to_binary()
  end

  def mysql_old_password(password, salt) do
    {p1, p2} = hash(password)
    {s1, s2} = hash(salt)
    seed1 = bxor(p1, s1)
    seed2 = bxor(p2, s2)
    list = rnd(9, seed1, seed2)
    {l, [extra]} = Enum.split(list, 8)
    l |> Enum.map(&bxor(&1, extra - 64)) |> to_string
  end

  defp hash(bin) when is_binary(bin), do: bin |> to_charlist |> hash
  defp hash(s), do: hash(s, 1_345_345_333, 305_419_889, 7)

  defp hash([c | s], n1, n2, add) do
    n1 = bxor(n1, (band(n1, 63) + add) * c + n1 * 256)
    n2 = n2 + bxor(n2 * 256, n1)
    add = add + c
    hash(s, n1, n2, add)
  end

  defp hash([], n1, n2, _add) do
    mask = bsl(1, 31) - 1
    {band(n1, mask), band(n2, mask)}
  end

  defp rnd(n, seed1, seed2) do
    mod = bsl(1, 30) - 1
    rnd(n, [], rem(seed1, mod), rem(seed2, mod))
  end

  defp rnd(0, list, _, _) do
    Enum.reverse(list)
  end

  defp rnd(n, list, seed1, seed2) do
    mod = bsl(1, 30) - 1
    seed1 = rem(seed1 * 3 + seed2, mod)
    seed2 = rem(seed1 + seed2 + 33, mod)
    float = seed1 / mod * 31
    val = trunc(float) + 64
    rnd(n - 1, [val | list], seed1, seed2)
  end
end
