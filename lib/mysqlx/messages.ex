defmodule Mysqlx.Messages do
  @moduledoc false

  import Record, only: [defrecord: 2]
  import Mysqlx.BinaryUtils

  require Decimal

  require Logger

  # @protocol_vsn_major 3
  # @protocol_vsn_minor 0

  defrecord :packet, [:size, :seqnum, :msg, :body]

  @commands [
    com_sleep: 0x00,
    com_quit: 0x01,
    com_init_db: 0x02,
    com_query: 0x03,
    com_field_list: 0x04,
    com_create_db: 0x05,
    com_drop_db: 0x06,
    com_refresh: 0x07,
    com_shutdown: 0x08,
    com_statistics: 0x09,
    com_process_info: 0x0A,
    com_connect: 0x0B,
    com_process_kill: 0x0C,
    com_debug: 0x0D,
    com_ping: 0x0E,
    com_time: 0x0F,
    com_delayed_inser: 0x10,
    com_change_use: 0x11,
    com_binlog_dump: 0x12,
    com_table_dump: 0x13,
    com_connect_out: 0x14,
    com_register_slave: 0x15,
    com_stmt_prepare: 0x16,
    com_stmt_execute: 0x17,
    com_stmt_send_long_data: 0x18,
    com_stmt_close: 0x19,
    com_stmt_reset: 0x1A,
    com_set_option: 0x1B,
    com_stmt_fetch: 0x1C,
    com_daemon: 0x1D,
    com_binlog_dump_gtid: 0x1E,
    com_reset_connection: 0x1F
  ]

  for {command, number} <- @commands do
    defmacro unquote(command)(), do: unquote(number)
  end

  @auth_types [
    ok: 0,
    kerberos: 2,
    cleartext: 3,
    md5: 5,
    scm: 6,
    gss: 7,
    sspi: 9,
    gss_cont: 8
  ]
  # Drop warning for now
  _ = @auth_types

  @error_fields [
    severity: ?S,
    code: ?C,
    message: ?M,
    detail: ?D,
    hint: ?H,
    position: ?P,
    internal_position: ?p,
    internal_query: ?q,
    where: ?W,
    schema: ?s,
    table: ?t,
    column: ?c,
    data_type: ?d,
    constraint: ?n,
    file: ?F,
    line: ?L,
    routine: ?R
  ]
  # Drop warning for now
  _ = @error_fields

  @types [
    float: [field_type_float: 0x04, field_type_double: 0x05],
    decimal: [field_type_decimal: 0x00, field_type_newdecimal: 0xF6],
    integer: [
      field_type_tiny: 0x01,
      field_type_short: 0x02,
      field_type_long: 0x03,
      field_type_int24: 0x09,
      field_type_year: 0x0D,
      field_type_longlong: 0x08
    ],
    timestamp: [field_type_timestamp: 0x07, field_type_datetime: 0x0C],
    date: [field_type_date: 0x0A],
    time: [field_type_time: 0x0B],
    bit: [field_type_bit: 0x10],
    string: [
      field_type_varchar: 0x0F,
      field_type_tiny_blob: 0xF9,
      field_type_medium_blob: 0xFA,
      field_type_long_blob: 0xFB,
      field_type_blob: 0xFC,
      field_type_var_string: 0xFD,
      field_type_string: 0xFE
    ],
    json: [field_type_json: 0xF5],
    geometry: [field_type_geometry: 0xFF],
    null: [field_type_null: 0x06]
  ]

  def __type__(:decode, _type, nil), do: nil

  for {_type, list} <- @types,
      {name, id} <- list do
    def __type__(:id, unquote(name)), do: unquote(id)
  end

  for {type, list} <- @types,
      {name, id} <- list do
    def __type__(:type, unquote(id)), do: {unquote(type), unquote(name)}
  end

  defrecord :msg_ok, [
    :affected_rows,
    :last_insert_id,
    :server_status,
    :warning_count,
    :info
  ]

  defrecord :msg_err, [
    :error_code,
    :state_marker,
    :sql_state,
    :error_message
  ]

  defrecord :msg_eof, [
    :warings,
    :status_flags,
    ## Since version MySQL of 5.7.X, MySQL sends bigger eof messages without any documentation, what is  added.
    :message
  ]

  defrecord :msg_handshake, [
    :protocol_version,
    :server_version,
    :connection_id,
    :auth_plugin_data_1,
    :capability_flags_1,
    :character_set,
    :status_flags,
    :capability_flags_2,
    :auth_plugin_data_2,
    :plugin
  ]

  defrecord :msg_handshake_response, [
    :client_capabilities,
    :max_packet_size,
    :character_set,
    :username,
    :password,
    :database
  ]

  defrecord :msg_ssl_request, [
    :client_capabilities,
    :max_packet_size,
    :character_set
  ]

  defrecord :msg_text_cmd, [
    :command,
    :statement
  ]

  defrecord :msg_column_count, [
    :column_count
  ]

  defrecord :msg_column_definition, [
    :catalog,
    :schema,
    :table,
    :org_table,
    :name,
    :org_name,
    :length_of_fixed_fields,
    :character_set,
    :max_column_size,
    :type,
    :flags,
    :decimals
  ]

  # defrecord :msg_text_row, [
  #   :row
  # ]

  def decode(
        <<len::size(24)-little-integer, seqnum::8, body::binary(len),
          rest::binary>>,
        state
      ) do
    msg = decode_msg(body, state)
    {packet(size: len, seqnum: seqnum, msg: msg, body: body), rest}
  end

  def decode(rest, _state), do: {nil, rest}

  def encode(msg, seqnum) do
    body = encode_msg(msg)
    <<byte_size(body)::24-little, seqnum::8, body::binary>>
  end

  # msg_err
  defp decode_msg(
         <<255::8, error_code::int16, ?#, sql_state::binary(5),
           error_message::binary>> = _body,
         _state
       ) do
    msg_err(
      error_code: error_code,
      state_marker: ?#,
      sql_state: sql_state,
      error_message: error_message
    )
  end

  # msg_ok
  defp decode_msg(<<0x00::8, rest::binary>> = _body, _state) do
    {affected_rows, rest} = length_encoded_integer(rest)
    {last_insert_id, rest} = length_encoded_integer(rest)

    <<server_status::little-size(16), warning_count::little-size(16),
      info::binary>> = rest

    msg_ok(
      affected_rows: affected_rows,
      last_insert_id: last_insert_id,
      server_status: server_status,
      warning_count: warning_count,
      info: info
    )
  end

  # msg_eof
  defp decode_msg(
         <<254::8, warings::little-size(16), status_flags::little-size(16),
           message::binary>> = _body,
         _state
       ) do
    msg_eof(
      warings: warings,
      status_flags: status_flags,
      message: message
    )
  end

  # msg_column_count
  defp decode_msg(body, :column_count) do
    {column_count, _} = length_encoded_integer(body)
    Logger.debug("column count: #{inspect(column_count)}")
    msg_column_count(column_count: column_count)
  end

  # msg_column_definition
  defp decode_msg(body, :column_definitions) do
    {catalog, rest} = length_encoded_string(body)
    {schema, rest} = length_encoded_string(rest)
    {table, rest} = length_encoded_string(rest)
    {org_table, rest} = length_encoded_string(rest)
    {name, rest} = length_encoded_string(rest)
    {org_name, rest} = length_encoded_string(rest)
    {length_of_fixed_fields, rest} = length_encoded_integer(rest)

    <<character_set::little-size(16), max_column_size::little-size(32), type::8,
      flags::little-size(16), decimals::8, _::little-size(16)>> = rest

    msg_column_definition(
      catalog: catalog,
      schema: schema,
      table: table,
      org_table: org_table,
      name: name,
      org_name: org_name,
      length_of_fixed_fields: length_of_fixed_fields,
      character_set: character_set,
      max_column_size: max_column_size,
      type: type,
      flags: flags,
      decimals: decimals
    )
  end

  # msg_handshake
  defp decode_msg(<<protocol_version::8, rest::binary>> = _body, _state) do
    Logger.debug("#{inspect(_state)}")
    [server_version, rest] = string_nul(rest)

    <<connection_id::little-size(32), auth_plugin_data_1::binary(8), 0::8,
      capability_flags_1::little-size(16), character_set::8,
      status_flags::little-size(16), capability_flags_2::little-size(16),
      rest::binary>> = rest

    {auth_plugin_data_2, rest} = auth_plugin_data_2(rest)
    [plugin, _] = string_nul(rest)

    msg_handshake(
      protocol_version: protocol_version,
      server_version: server_version,
      connection_id: connection_id,
      auth_plugin_data_1: auth_plugin_data_1,
      capability_flags_1: capability_flags_1,
      character_set: character_set,
      status_flags: status_flags,
      capability_flags_2: capability_flags_2,
      auth_plugin_data_2: auth_plugin_data_2,
      plugin: plugin
    )
  end

  # # msg_text_row
  # defp decode_msg(
  #        body,
  #        :text_rows
  #      ) do
  #   nil
  # end

  # msg_ssl_request
  defp encode_msg(
         msg_ssl_request(
           client_capabilities: client_capabilities,
           max_packet_size: max_packet_size,
           character_set: character_set
         )
       ) do
    <<client_capabilities::little-size(32), max_packet_size::little-size(32),
      character_set::8, 0::23*8>>
  end

  # msg_handshake_response
  defp encode_msg(
         msg_handshake_response(
           client_capabilities: client_capabilities,
           max_packet_size: max_packet_size,
           character_set: character_set,
           username: username,
           password: password,
           database: database
         )
       ) do
    <<client_capabilities::little-size(32), max_packet_size::little-size(32),
      character_set::8, 0::23*8, username::binary, 0::8, byte_size(password)::8,
      password::binary, database::binary, 0::8>>
  end

  # msg_text_cmd
  defp encode_msg(
         msg_text_cmd(
           command: command,
           statement: statement
         )
       ) do
    <<command::8, statement::binary>>
  end

  def decode_text_rows(
        <<len::size(24)-little-integer, seqnum::size(8)-integer,
          body::size(len)-binary, rest::binary>>,
        fields,
        rows,
        json_library
      ) do
    case body do
      # eof
      <<254::8, _::binary>> = body when byte_size(body) < 9 ->
        msg = decode_msg(body, :text_rows)

        {:ok, packet(size: len, seqnum: seqnum, msg: msg, body: body), rows,
         rest}

      # row
      body ->
        Logger.debug("#{inspect("here")}")
        row = Mysqlx.RowParser.decode_text_rows(body, fields, json_library)
        Logger.debug("#{inspect(row)}")
        decode_text_rows(rest, fields, [row | rows], json_library)
    end
  end

  def decode_text_rows(<<rest::binary>>, _fields, rows, _json_library) do
    {:more, rows, rest}
  end

  # Due to Bug#59453(http://bugs.mysql.com/bug.php?id=59453)
  # the auth-plugin-name is missing the terminating
  # NUL-char in versions prior to 5.5.10 and 5.6.2.
  def auth_plugin_data_2(<<length_auth_plugin_data::8, _::80, next::binary>>) do
    length = max(13, length_auth_plugin_data - 8)
    length_nul_terminated = length - 1
    <<null_terminated?::size(length)-binary, next::binary>> = next

    auth_plugin_data_2 =
      case null_terminated? do
        <<contents::size(length_nul_terminated)-binary, 0::8>> -> contents
        contents -> contents
      end

    {String.trim(auth_plugin_data_2, "\0"), next}
  end

  # string<NUL>
  defp string_nul(bin) do
    :binary.split(bin, <<0>>)
  end

  # string<lenenc>
  defp length_encoded_string(bin) do
    {length, next} = length_encoded_integer(bin)
    <<string::size(length)-binary, next::binary>> = next
    {string, next}
  end

  # int<lenenc>
  defp length_encoded_integer(bin) do
    case bin do
      <<value::8, rest::binary>> when value <= 250 -> {value, rest}
      <<252::8, value::16-little, rest::bits>> -> {value, rest}
      <<253::8, value::24-little, rest::bits>> -> {value, rest}
      <<254::8, value::64-little, rest::bits>> -> {value, rest}
    end
  end

  defp to_length_encoded_integer(int) do
    case int do
      int when int <= 250 -> <<int::8>>
      int when int <= 65_535 -> <<252::8, int::16-little>>
      int when int <= 16_777_215 -> <<253::8, int::24-little>>
      int -> <<254::8, int::64-little>>
    end
  end
end
