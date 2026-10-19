use "collections"
use "cli"
use "net"
use "time"
use pg = "postgres"

use @access[I32](path: Pointer[U8] tag, mode: I32)

actor \nodoc\ _SessionShutdownTest is (pg.SessionStatusNotify &
  pg.PrepareReceiver & pg.ResultReceiver)
  let _env: Env
  let _session: pg.Session
  var _next: U8 = 0

  new create(auth: TCPConnectAuth, config: _Config, env: Env) =>
    _env = env
    _session = pg.Session(
      pg.ServerConnectInfo(auth, config.dbhost, config.dbport),
      pg.DatabaseConnectInfo(config.user, config.password, config.database),
      this)

  be pg_session_authenticated(session: pg.Session) =>
    session.execute(pg.PreparedQuery("SELECT pg_sleep(2)", _P.none()), this)
    session.prepare("later", "SELECT 1", this)
    session.execute(pg.PreparedQuery("SELECT 1", _P.none()), this)
    session.close()
    session.prepare("after", "SELECT 2", this)
    session.execute(pg.PreparedQuery("SELECT 2", _P.none()), this)

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    _fail("closed query succeeded")

  be pg_query_failed(session: pg.Session, query': pg.Query,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    if not (failure is pg.SessionClosed) then
      _fail("wrong query failure")
      return
    end
    let sql = try (query' as pg.PreparedQuery).string
      else "" end
    if ((_next == 0) and (sql == "SELECT pg_sleep(2)")) or
      ((_next == 2) and (sql == "SELECT 1")) or
      ((_next == 5) and (sql == "SELECT 2")) then
      _next = _next + 1
      if _next == 6 then
        _env.out.print("session shutdown settled commands")
      end
    else _fail("wrong query failure order") end

  be pg_statement_prepared(session: pg.Session, name: String) =>
    _fail("closed preparation succeeded")

  be pg_prepare_failed(session: pg.Session, name: String,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    if (_next == 1) and (name == "later") and
      (failure is pg.SessionClosed) then
      _next = 2
    elseif (_next == 4) and (name == "after") and
      (failure is pg.SessionClosed) then
      _next = 5
    else _fail("wrong preparation failure order") end

  be pg_transaction_status(session: pg.Session,
    status: pg.TransactionStatus) => None

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _fail("session connection failed")

  be pg_session_shutdown(session: pg.Session) =>
    if _next == 3 then _next = 4
    else _fail("shutdown before command failures") end

  fun ref _fail(message: String) =>
    _env.err.print(message)
    _env.exitcode(1)
    _session.close()

actor \nodoc\ _ConnectionNulTest is pg.SessionStatusNotify
  let _env: Env
  var _failed: U8 = 0
  var _shutdowns: U8 = 0

  new create(auth: TCPConnectAuth, config: _Config, env: Env) =>
    _env = env
    let host = pg.ServerConnectInfo(auth, config.dbhost, config.dbport)
    let database = pg.DatabaseConnectInfo(config.user, config.password,
      config.database)
    pg.Session(pg.ServerConnectInfo(auth, config.dbhost + "\0bad",
      config.dbport), database, this)
    pg.Session(pg.ServerConnectInfo(auth, config.dbhost,
      config.dbport + "\0bad"), database, this)
    pg.Session(host, pg.DatabaseConnectInfo(config.user + "\0bad",
      config.password, config.database), this)
    pg.Session(host, pg.DatabaseConnectInfo(config.user,
      config.password + "\0bad", config.database), this)
    pg.Session(host, pg.DatabaseConnectInfo(config.user, config.password,
      config.database + "\0bad"), this)

  be pg_session_authenticated(session: pg.Session) =>
    _fail("truncated connection field was accepted")
    session.close()

  be pg_transaction_status(session: pg.Session,
    status: pg.TransactionStatus) => None

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _failed = _failed + 1
    _complete()

  be pg_session_shutdown(session: pg.Session) =>
    _shutdowns = _shutdowns + 1
    _complete()

  fun ref _complete() =>
    if (_failed == 5) and (_shutdowns == 5) then
      _env.out.print("connection fields rejected NUL")
    end

  fun ref _fail(message: String) =>
    _env.err.print(message)
    _env.exitcode(1)

actor \nodoc\ _StartupShutdownTest is (pg.SessionStatusNotify &
  pg.PrepareReceiver & pg.ResultReceiver)
  let _env: Env
  let _session: pg.Session
  let _marker: String
  let _timers: Timers = Timers
  var _next: U8 = 0

  new create(auth: TCPConnectAuth, config: _Config, env: Env) =>
    _env = env
    _session = pg.Session(
      pg.ServerConnectInfo(auth, config.dbhost, config.dbport),
      pg.DatabaseConnectInfo(config.user, config.password, config.database),
      this)
    _marker = try EnvVars(env.vars)("CONDUIT_DNS_MARKER")?
      else "" end
    _session.execute(pg.PreparedQuery("SELECT 1", _P.none()), this)
    _session.prepare("queued", "SELECT 1", this)
    this.check_marker()

  be check_marker() =>
    if (_marker.size() > 0) and (@access(_marker.cstring(), 0) == 0) then
      _session.close()
      _session.prepare("after", "SELECT 2", this)
      _session.execute(pg.PreparedQuery("SELECT 2", _P.none()), this)
    else
      _timers(Timer(_CheckDNSMarker(this), 1_000_000))
    end

  be pg_session_authenticated(session: pg.Session) =>
    _fail("startup completed after close")

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    _fail("closed query succeeded")

  be pg_query_failed(session: pg.Session, query': pg.Query,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    let sql = try (query' as pg.PreparedQuery).string
      else "" end
    if ((_next == 0) and (sql == "SELECT 1")) or
      ((_next == 5) and (sql == "SELECT 2")) then
      if not (failure is pg.SessionClosed) then
        _fail("wrong startup query failure")
        return
      end
      _next = _next + 1
      if sql == "SELECT 2" then
        _env.out.print("startup shutdown settled commands")
      end
    else _fail("wrong startup query failure") end

  be pg_statement_prepared(session: pg.Session, name: String) =>
    _fail("closed preparation succeeded")

  be pg_prepare_failed(session: pg.Session, name: String,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    if (_next == 1) and (name == "queued") and
      (failure is pg.SessionClosed) then
      _next = 2
    elseif (_next == 4) and (name == "after") and
      (failure is pg.SessionClosed) then
      _next = 5
    else _fail("wrong startup preparation failure") end

  be pg_transaction_status(session: pg.Session,
    status: pg.TransactionStatus) =>
    _fail("closed startup reported ready")

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    if _next == 2 then _next = 3
    else _fail("wrong startup lifecycle order") end

  be pg_session_shutdown(session: pg.Session) =>
    if _next == 3 then _next = 4
    else _fail("shutdown before startup failures") end

  fun ref _fail(message: String) =>
    _env.err.print(message)
    _env.exitcode(1)
    _session.close()

class \nodoc\ _CheckDNSMarker is TimerNotify
  let _test: _StartupShutdownTest

  new iso create(test': _StartupShutdownTest) => _test = test'

  fun ref apply(timer: Timer, count: U64): Bool =>
    _test.check_marker()
    false

actor \nodoc\ _QueueDrainTest is (pg.SessionStatusNotify &
  pg.ResultReceiver)
  let _env: Env
  let _session: pg.Session
  var _failures: USize = 0
  var _first: Bool = false

  new create(auth: TCPConnectAuth, config: _Config, env: Env) =>
    _env = env
    _session = pg.Session(
      pg.ServerConnectInfo(auth, config.dbhost, config.dbport),
      pg.DatabaseConnectInfo(config.user, config.password, config.database),
      this)

  be pg_session_authenticated(session: pg.Session) =>
    session.execute(pg.PreparedQuery("SELECT pg_sleep(3)", _P.none()), this)
    let params: Array[pg.FieldDataTypes] val = [as pg.FieldDataTypes:
      "x\0y"]
    let rejected = pg.PreparedQuery("SELECT $1", params)
    for _ in Range(0, 40_000) do session.execute(rejected, this) end
    session.execute(pg.PreparedQuery("SELECT 1", _P.none()), this)

  be pg_query_result(session: pg.Session, result: pg.Result) =>
    let sql = try (result.query() as pg.PreparedQuery).string
      else "" end
    if (sql == "SELECT pg_sleep(3)") and not _first then
      _first = true
    elseif (sql == "SELECT 1") and _first and (_failures == 40_000) then
      _env.out.print("deep rejection queue drained")
      _session.close()
    else _fail("wrong queue result") end

  be pg_query_failed(session: pg.Session, query': pg.Query,
    failure: (pg.ErrorResponseMessage | pg.ClientQueryError))
  =>
    if (failure is pg.DataError) and (_failures < 40_000) then
      _failures = _failures + 1
    else _fail("wrong queue failure") end

  be pg_transaction_status(session: pg.Session,
    status: pg.TransactionStatus) =>
    if not (status is pg.TransactionIdle) then
      _fail("wrong queue transaction status")
    end

  be pg_session_connection_failed(session: pg.Session,
    reason: pg.ConnectionFailureReason)
  =>
    _fail("session connection failed")

  be pg_session_shutdown(session: pg.Session) => None

  fun ref _fail(message: String) =>
    _env.err.print(message)
    _env.exitcode(1)
    _session.close()
