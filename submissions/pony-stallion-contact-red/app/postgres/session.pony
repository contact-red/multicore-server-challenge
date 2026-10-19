use "collections"
use net = "net"
use "lib:pq"

use @pony_asio_event_create[AsioEventID](owner: AsioEventNotify,
  fd: U32, flags: U32, nsec: U64, noisy: Bool)
use @conduit_pq_start[I32](host: Pointer[U8] tag, port: Pointer[U8] tag,
  user: Pointer[U8] tag, password: Pointer[U8] tag,
  database: Pointer[U8] tag)
use @conduit_pq_take[Pointer[None] tag](socket: I32)
use @conduit_pq_cancel[None](socket: I32)
use @conduit_pq_timestamp[I64](value: Pointer[U8] tag)
use @PQconnectPoll[I32](conn: Pointer[None] tag)
use @PQstatus[I32](conn: Pointer[None] tag)
use @PQsocket[I32](conn: Pointer[None] tag)
use @PQsetnonblocking[I32](conn: Pointer[None] tag, value: I32)
use @PQsendPrepare[I32](conn: Pointer[None] tag, name: Pointer[U8] tag,
  sql: Pointer[U8] tag, count: I32, types: Pointer[U32] tag)
use @PQsendQueryPrepared[I32](conn: Pointer[None] tag,
  name: Pointer[U8] tag, count: I32,
  values: Pointer[Pointer[U8] tag] tag, lengths: Pointer[I32] tag,
  formats: Pointer[I32] tag, result_format: I32)
use @PQsendQueryParams[I32](conn: Pointer[None] tag,
  sql: Pointer[U8] tag, count: I32, types: Pointer[U32] tag,
  values: Pointer[Pointer[U8] tag] tag, lengths: Pointer[I32] tag,
  formats: Pointer[I32] tag, result_format: I32)
use @PQflush[I32](conn: Pointer[None] tag)
use @PQconsumeInput[I32](conn: Pointer[None] tag)
use @PQisBusy[I32](conn: Pointer[None] tag)
use @PQgetResult[Pointer[None] tag](conn: Pointer[None] tag)
use @PQresultStatus[I32](result: Pointer[None] tag)
use @PQresultErrorField[Pointer[U8] val](result: Pointer[None] tag,
  code: I32)
use @PQresultErrorMessage[Pointer[U8] val](result: Pointer[None] tag)
use @PQntuples[I32](result: Pointer[None] tag)
use @PQnfields[I32](result: Pointer[None] tag)
use @PQftype[U32](result: Pointer[None] tag, column: I32)
use @PQgetisnull[I32](result: Pointer[None] tag, row: I32, column: I32)
use @PQgetvalue[Pointer[U8] val](result: Pointer[None] tag,
  row: I32, column: I32)
use @PQclear[None](result: Pointer[None] tag)
use @PQtransactionStatus[I32](conn: Pointer[None] tag)
use @PQfinish[None](conn: Pointer[None] tag)

class val _Prepare
  let name: String
  let sql: String
  let receiver: PrepareReceiver

  new val create(name': String, sql': String, receiver': PrepareReceiver) =>
    name = name'
    sql = sql'
    receiver = receiver'

class val _Execute
  let query: Query
  let receiver: ResultReceiver

  new val create(query': Query, receiver': ResultReceiver) =>
    query = query'
    receiver = receiver'

type _Command is (_Prepare | _Execute)

actor Session is AsioEventNotify
  """Runs prepared and parameterized queries serially over libpq."""
  let _notify: SessionStatusNotify
  var _conn: Pointer[None] tag = Pointer[None]
  var _event: AsioEventID = AsioEvent.none()
  var _fd: I32 = -1
  var _writing: Bool = false
  var _resolving: Bool = false
  var _ready: Bool = false
  var _closed: Bool = false
  var _current: (_Command | None) = None
  var _reported: Bool = false
  embed _queue: List[_Command] = List[_Command]

  new create(server: ServerConnectInfo, database: DatabaseConnectInfo,
    notify: SessionStatusNotify)
  =>
    """Start a connection without blocking the Pony scheduler on DNS."""
    _notify = notify
    if server.host.contains("\0") or server.port.contains("\0") or
      database.user.contains("\0") or database.password.contains("\0") or
      database.database.contains("\0") then
      _shutdown()
      return
    end
    _fd = @conduit_pq_start(server.host.cstring(), server.port.cstring(),
      database.user.cstring(), database.password.cstring(),
      database.database.cstring())
    if _fd < 0 then
      _shutdown()
    else
      _resolving = true
      _event = @pony_asio_event_create(this, _fd.u32(),
        AsioEvent.read() or AsioEvent.oneshot(), 0, true)
    end

  be prepare(name: String, sql: String, receiver: PrepareReceiver) =>
    """Queue a named statement for preparation."""
    if _closed then
      receiver.pg_prepare_failed(this, name, SessionClosed)
    else
      _queue.push(_Prepare(name, sql, receiver))
      _start_next()
    end

  be execute(query': Query, receiver: ResultReceiver) =>
    """Queue a query behind any operation already in flight."""
    if _closed then
      receiver.pg_query_failed(this, query', SessionClosed)
    else
      _queue.push(_Execute(query', receiver))
      _start_next()
    end

  be close() =>
    """Close the connection and release its socket."""
    _shutdown()

  be _run_next() =>
    _start_next()

  be _event_notify(event: AsioEventID, flags: U32, arg: U32) =>
    if AsioEvent.disposable(flags) then
      net.PonyAsio.destroy(event)
      return
    end
    if _closed or not (event is _event) then return end
    if AsioEvent.errored(flags) then
      _shutdown()
      return
    end
    if _resolving then
      net.PonyAsio.unsubscribe(_event)
      _event = AsioEvent.none()
      _conn = @conduit_pq_take(_fd)
      _fd = -1
      _resolving = false
      if _conn.is_null() then _shutdown()
      else _drive_connect() end
    elseif not _ready then
      _drive_connect()
    else
      if AsioEvent.writeable(flags) then _drive_flush() end
      if AsioEvent.readable(flags) then _drive_read() end
    end

  fun ref _arm(write': Bool) =>
    let fd = @PQsocket(_conn)
    if fd < 0 then
      _shutdown()
      return
    end
    if (fd != _fd) or _event.is_null() or (write' != _writing) then
      if not _event.is_null() then net.PonyAsio.unsubscribe(_event) end
      _fd = fd
      _writing = write'
      _event = @pony_asio_event_create(this, fd.u32(),
        (if write' then AsioEvent.write() else AsioEvent.read() end)
          or AsioEvent.oneshot(), 0, true)
    elseif write' then
      net.PonyAsio.set_unwriteable(_event)
      net.PonyAsio.resubscribe_write(_event)
    else
      net.PonyAsio.set_unreadable(_event)
      net.PonyAsio.resubscribe_read(_event)
    end

  fun ref _drive_connect() =>
    match @PQconnectPoll(_conn)
    | 1 => _arm(false)
    | 2 => _arm(true)
    | 3 =>
      if (@PQstatus(_conn) != 0) or (@PQsetnonblocking(_conn, 1) != 0) then
        _shutdown()
        return
      end
      _ready = true
      _notify.pg_session_authenticated(this)
      _notify.pg_transaction_status(this, TransactionIdle)
      _start_next()
    else _shutdown()
    end

  fun ref _start_next() =>
    if not _ready or (_current isnt None) then return end
    if _queue.size() == 0 then
      _arm(false)
      return
    end
    try
      let command = _queue.shift()?
      _current = command
      _reported = false
      match command
      | let prepare': _Prepare =>
        if prepare'.name.contains("\0") or prepare'.sql.contains("\0") then
          prepare'.receiver.pg_prepare_failed(this, prepare'.name, DataError)
          _current = None
          _report_status()
          if not _closed then this._run_next() end
          return
        end
        if @PQsendPrepare(_conn, prepare'.name.cstring(),
          prepare'.sql.cstring(), 0, Pointer[U32]) != 1 then
          _shutdown()
          return
        end
      | let execute': _Execute =>
        if not _send(execute'.query) then return end
      end
      _drive_flush()
    end

  fun ref _send(query': Query): Bool =>
    match query'
    | let named: NamedPreparedQuery =>
      if named.name.contains("\0") then
        _data_error()
        return false
      end
    | let plain: PreparedQuery =>
      if plain.string.contains("\0") then
        _data_error()
        return false
      end
    end
    let params = match query'
      | let named: NamedPreparedQuery => named.params
      | let plain: PreparedQuery => plain.params
      end
    if params.size() == 0 then
      let sent = match query'
        | let named: NamedPreparedQuery =>
          @PQsendQueryPrepared(_conn, named.name.cstring(), 0,
            Pointer[Pointer[U8] tag], Pointer[I32], Pointer[I32], 0)
        | let plain: PreparedQuery =>
          @PQsendQueryParams(_conn, plain.string.cstring(), 0,
            Pointer[U32], Pointer[Pointer[U8] tag], Pointer[I32],
            Pointer[I32], 0)
        end
      if sent != 1 then _shutdown() end
      return sent == 1
    end
    let strings = Array[String](params.size())
    let values = Array[Pointer[U8] tag](params.size())
    for param in params.values() do
      match param
      | None =>
        values.push(Pointer[U8])
      | let s: String =>
        if s.contains("\0") then
          _data_error()
          return false
        end
        strings.push(s)
        values.push(s.cstring())
      | let n: I64 =>
        let s: String val = n.string()
        strings.push(s)
        values.push(s.cstring())
      | let n: I32 =>
        let s: String val = n.string()
        strings.push(s)
        values.push(s.cstring())
      | let _: PgArray =>
        _data_error()
        return false
      end
    end
    let sent = match query'
      | let named: NamedPreparedQuery =>
        @PQsendQueryPrepared(_conn, named.name.cstring(), params.size().i32(),
          values.cpointer(), Pointer[I32], Pointer[I32], 0)
      | let plain: PreparedQuery =>
        @PQsendQueryParams(_conn, plain.string.cstring(), params.size().i32(),
          Pointer[U32], values.cpointer(), Pointer[I32], Pointer[I32], 0)
      end
    if sent != 1 then _shutdown() end
    sent == 1

  fun ref _data_error() =>
    match _current
    | let execute': _Execute =>
      execute'.receiver.pg_query_failed(this, execute'.query, DataError)
    end
    _reported = true
    _current = None
    _report_status()
    if not _closed then this._run_next() end

  fun ref _report_status() =>
    match @PQtransactionStatus(_conn)
    | 0 => _notify.pg_transaction_status(this, TransactionIdle)
    | 2 => _notify.pg_transaction_status(this, TransactionInBlock)
    | 3 => _notify.pg_transaction_status(this, TransactionFailed)
    else _shutdown()
    end

  fun ref _drive_flush() =>
    match @PQflush(_conn)
    | 0 => _arm(false)
    | 1 => _arm(true)
    else _shutdown()
    end

  fun ref _drive_read() =>
    if @PQconsumeInput(_conn) != 1 then
      _shutdown()
      return
    end
    if _current is None then
      _arm(false)
      return
    end
    if @PQisBusy(_conn) != 0 then
      _arm(false)
      return
    end
    let raw = @PQgetResult(_conn)
    if raw.is_null() then
      if not _reported then _shutdown(); return end
      _current = None
      _report_status()
      if not _closed then _start_next() end
      return
    end
    _handle(raw)
    @PQclear(raw)
    if not _closed then _drive_read() end

  fun ref _handle(raw: Pointer[None] tag) =>
    let status = @PQresultStatus(raw)
    match _current
    | let prepare': _Prepare =>
      if status == 1 then
        prepare'.receiver.pg_statement_prepared(this, prepare'.name)
      else
        prepare'.receiver.pg_prepare_failed(this, prepare'.name,
          _failure(raw))
      end
    | let execute': _Execute =>
      if (status == 1) or (status == 2) then
        if status == 2 then
          try
            execute'.receiver.pg_query_result(this,
              ResultSet(execute'.query, _decode(raw)?))
          else
            execute'.receiver.pg_query_failed(this, execute'.query,
              DataError)
          end
        else
          execute'.receiver.pg_query_result(this,
            SimpleResult(execute'.query))
        end
      else
        execute'.receiver.pg_query_failed(this, execute'.query,
          _failure(raw))
      end
    end
    _reported = true

  fun _failure(raw: Pointer[None] tag)
    : (ErrorResponseMessage | ClientQueryError)
  =>
    let code = @PQresultErrorField(raw, 67)
    if code.is_null() then return DataError end
    ErrorResponseMessage(_copy(code),
      _copy(@PQresultErrorMessage(raw)))

  fun _copy(ptr: Pointer[U8] val): String val =>
    recover val String.copy_cstring(ptr) end

  fun _decode(raw: Pointer[None] tag): Rows ? =>
    let rows = recover iso Array[Row] end
    var row: I32 = 0
    while row < @PQntuples(raw) do
      let fields = recover iso Array[Field] end
      var column: I32 = 0
      while column < @PQnfields(raw) do
        let value: FieldData = if @PQgetisnull(raw, row, column) != 0 then
          None
        else
          let text': String val = _copy(@PQgetvalue(raw, row, column))
          match @PQftype(raw, column)
          | 16 => text' == "t"
          | 20 => text'.i64()?
          | 21 | 23 => text'.i32()?
          | 1114 | 1184 =>
            if text' == "infinity" then
              PgTimestamp(I64.max_value())
            elseif text' == "-infinity" then
              PgTimestamp(I64.min_value())
            else
              let us = @conduit_pq_timestamp(text'.cstring())
              if us == I64.min_value() then error end
              PgTimestamp(us)
            end
          | 1009 => _decode_array(text')?
          else text'
          end
        end
        fields.push(Field(value))
        column = column + 1
      end
      rows.push(Row(consume fields))
      row = row + 1
    end
    consume rows

  fun _decode_array(value: String): PgArray ? =>
    if (value.size() < 2) or (value(0)? != '{') or
      (value(value.size() - 1)? != '}') then error end
    let elements = recover iso Array[(FieldData | None)] end
    var part = String
    var quoted = false
    var escaped = false
    var was_quoted = false
    var i: USize = 1
    while i < (value.size() - 1) do
      let ch = value(i)?
      if escaped then
        part.push(ch)
        escaped = false
      elseif quoted and (ch == '\\') then
        escaped = true
      elseif ch == '"' then
        quoted = not quoted
        was_quoted = true
      elseif ((ch == '{') or (ch == '}')) and not quoted then
        error
      elseif (ch == ',') and not quoted then
        if (part == "NULL") and not was_quoted then
          elements.push(None)
        else elements.push(part.clone()) end
        part.clear()
        was_quoted = false
      else part.push(ch)
      end
      i = i + 1
    end
    if quoted or escaped then error end
    if value.size() > 2 then
      if (part == "NULL") and not was_quoted then
        elements.push(None)
      else elements.push(part.clone()) end
    end
    PgArray(25, consume elements)

  fun ref _shutdown() =>
    if _closed then return end
    _closed = true
    if not _event.is_null() then
      net.PonyAsio.unsubscribe(_event)
      _event = AsioEvent.none()
    end
    if _resolving then
      @conduit_pq_cancel(_fd)
    elseif not _conn.is_null() then
      @PQfinish(_conn)
    end
    match _current
    | let prepare': _Prepare if not _reported =>
      prepare'.receiver.pg_prepare_failed(this, prepare'.name, SessionClosed)
    | let execute': _Execute if not _reported =>
      execute'.receiver.pg_query_failed(this, execute'.query, SessionClosed)
    end
    _current = None
    for command in _queue.values() do
      match command
      | let prepare': _Prepare =>
        prepare'.receiver.pg_prepare_failed(this, prepare'.name,
          SessionClosed)
      | let execute': _Execute =>
        execute'.receiver.pg_query_failed(this, execute'.query,
          SessionClosed)
      end
    end
    _queue.clear()
    if not _ready then
      _notify.pg_session_connection_failed(this, ConnectionError)
    end
    _notify.pg_session_shutdown(this)
