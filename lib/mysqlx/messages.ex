defmodule Mysqlx.Messages do
  @moduledoc false

  import Record, only: [defrecord: 2]
  import Mysqlx.BinaryUtils

  require Decimal

  require Logger

  # @protocol_vsn_major 3
  # @protocol_vsn_minor 0

  defrecord :packet, [:size, :seqnum, :msg, :body]

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

  # msg_handshake
  defp decode_msg(<<protocol_version::8, rest::binary>> = _body, _state) do
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
