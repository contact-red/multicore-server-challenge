use net = "net"

type FieldData is (String | I64 | I32 | F64 | Bool | None | PgArray |
  PgTimestamp)
  """Values decoded from PostgreSQL result columns."""

type FieldDataTypes is (String | I64 | I32 | None | PgArray)
  """Values accepted as PostgreSQL query parameters."""

class val PgArray
  """A one-dimensional PostgreSQL array."""
  let element_oid: U32
  let elements: Array[(FieldData | None)] val

  new val create(oid: U32, values: Array[(FieldData | None)] val) =>
    """The OID identifies the element type, not the array type."""
    element_oid = oid
    elements = values

class val PgTimestamp
  """
  Microseconds since the PostgreSQL epoch. I64.min_value() and
  I64.max_value() represent negative and positive infinity.
  """
  let microseconds: I64

  new val create(value: I64) =>
    """Use PostgreSQL's 2000-01-01 UTC epoch."""
    microseconds = value

class val Field
  """A decoded column value."""
  let value: FieldData

  new val create(value': FieldData) =>
    """Represent SQL NULL with None."""
    value = value'

class val Row
  """One row of decoded columns."""
  let fields: Array[Field] val

  new val create(fields': Array[Field] val) =>
    """Keep columns in SELECT result order."""
    fields = fields'

type Rows is Array[Row] val
  """Rows returned by a PostgreSQL query."""

class val NamedPreparedQuery
  """An invocation of a named prepared statement."""
  let name: String
  let params: Array[FieldDataTypes] val

  new val create(name': String, params': Array[FieldDataTypes] val) =>
    """Use a name previously registered with Session.prepare."""
    name = name'
    params = params'

class val PreparedQuery
  """A single SQL statement with parameters."""
  let string: String
  let params: Array[FieldDataTypes] val

  new val create(string': String, params': Array[FieldDataTypes] val) =>
    """Session.execute sends this SQL without registering a name."""
    string = string'
    params = params'

type Query is (NamedPreparedQuery | PreparedQuery)
  """A prepared statement invocation or parameterized SQL statement."""

class val ResultSet
  """Rows returned by a query."""
  let _query: Query
  let _rows: Rows

  new val create(query': Query, rows': Rows) =>
    """The query identifies this result in receiver callbacks."""
    _query = query'
    _rows = rows'

  fun query(): Query =>
    """Return the query that produced these rows."""
    _query

  fun rows(): Rows =>
    """Return the decoded rows."""
    _rows

class val SimpleResult
  """A successful query that returned no rows."""
  let _query: Query

  new val create(query': Query) =>
    """The query identifies this result in receiver callbacks."""
    _query = query'

  fun query(): Query =>
    """Return the completed query."""
    _query

type Result is (ResultSet | SimpleResult)
  """A row-bearing or command-only query result."""

class val ErrorResponseMessage
  """A PostgreSQL error with its SQLSTATE and message."""
  let code: String
  let message: String

  new val create(code': String, message': String) =>
    """The SQLSTATE remains available separately for error handling."""
    code = code'
    message = message'

  fun string(): String =>
    """Return the SQLSTATE followed by the server message."""
    code + ": " + message

primitive SessionClosed
  """A command cannot complete because the session is closed."""

  fun string(): String =>
    """Return a message that the session is closed."""
    "Session closed"

primitive DataError
  """The client rejected an input or could not interpret a result."""

  fun string(): String =>
    """Return a message for invalid input or an unreadable result."""
    "Invalid database input or result"

primitive ProtocolViolation
  """The connection returned an unexpected result."""

  fun string(): String =>
    """Return a message for an unusable server response."""
    "Unexpected database result"

type ClientQueryError is (SessionClosed | DataError | ProtocolViolation)
  """Failures detected by the client before or while reading a query."""

primitive ConnectionError
  """The PostgreSQL connection did not become ready."""

  fun string(): String =>
    """Return a message for connection failure before readiness."""
    "Database connection failed"

type ConnectionFailureReason is ConnectionError
  """Reasons a session can fail before it becomes ready."""

primitive TransactionIdle
  """The connection is outside a transaction."""

primitive TransactionInBlock
  """The connection is inside a transaction."""

primitive TransactionFailed
  """The current transaction has failed."""

type TransactionStatus is
  (TransactionIdle | TransactionInBlock | TransactionFailed)
  """The server's transaction state at readiness or after an operation."""

interface tag ResultReceiver
  """Receives a result or failure for each query."""
  be pg_query_result(session: Session, result: Result)
    """Receive a successful query result."""
  be pg_query_failed(session: Session, query': Query,
    failure: (ErrorResponseMessage | ClientQueryError))
    """Receive a query failure."""

interface tag PrepareReceiver
  """Receives a result or failure for each statement preparation."""
  be pg_statement_prepared(session: Session, name: String)
    """Receive a successful statement preparation."""
  be pg_prepare_failed(session: Session, name: String,
    failure: (ErrorResponseMessage | ClientQueryError))
    """Receive a statement preparation failure."""

interface tag SessionStatusNotify
  """Receives connection and transaction state changes."""
  be pg_session_authenticated(session: Session)
    """Receive notice that the connection is ready."""
  be pg_transaction_status(session: Session, status: TransactionStatus)
    """Receive the transaction state at readiness or after an operation."""
  be pg_session_connection_failed(session: Session,
    reason: ConnectionFailureReason)
    """Receive a connection failure before readiness."""
  be pg_session_shutdown(session: Session)
    """Receive notice that the session has closed."""

class val ServerConnectInfo
  """The database server address."""
  let host: String
  let port: String

  new val create(auth: net.TCPConnectAuth, host': String, port': String) =>
    """Accept a DNS name or IP address for the libpq connection."""
    host = host'
    port = port'

class val DatabaseConnectInfo
  """The database credentials and name."""
  let user: String
  let password: String
  let database: String

  new val create(user': String, password': String, database': String) =>
    """Pass these fields to libpq as separate connection options."""
    user = user'
    password = password'
    database = database'
