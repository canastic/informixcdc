from typing import (
    Iterable,
    Any,
    Sequence,
    Optional,
    NamedTuple,
    List,
    Dict,
    cast,
    Callable,
    Awaitable,
    AsyncIterable,
)
from typing_extensions import Protocol
from datetime import datetime, timedelta
from collections import namedtuple


class Record:
    seq: int

    def __init__(self, seq: int):
        self.seq = seq


class BeginTx(Record):
    start_at: datetime

    def __init__(self, seq: int, start_at: datetime) -> None:
        super().__init__(seq)
        self.start_at = start_at


class Cursor(Protocol):
    """The protocol used to communicate with Informix. aioodbc cursors implement
    it.
    """

    async def execute(
            self,
            query: str,
            params: Optional[Sequence[Any]] = None,
    ) -> Any:
        ...

    async def fetchval(self) -> Optional[Any]:
        ...

    async def fetchone(self) -> Optional[Any]:
        ...


TableColumns = NamedTuple("Table", [
    ("table", str),
    ("columns", List[str]),
])


async def records(
        cursor: Cursor,
        server: str,
        tables: Iterable[TableColumns],
        from_seq: Optional[int] = None,
        read_timeout: Optional[timedelta] = None,
        max_records: int = 100,
) -> AsyncIterable[Record]:
    """Starts CDC and get records from the given cursor.

    Args:
        cursor: A cursor into an Informix connection. Take it from
            ``aioodbc``.

        tables: A description of the tables and columns to capture.

        from_seq: If provided, will start capturing data from the given
            sequence number. Can be taken from a previously captured 
            ``Record``.

        read_timeout: Internal knob: time until a single call to the
            database to retrieve records times out. After it passes, the
            iterator will just make another call.

        max_records: Internal knob: the maximum amount of records to capture
            in a single call to the database.

    Returns:
        An iterator that continuously yields CDC records until there's an error.

    Raises:
        CDCError
        UnknownCDCErrorCode
    """

    error_codes = await _load_error_codes(cursor)
    execute_fetch_int = _make_execute_fetch_int(cursor, error_codes)

    timeout: int = -1 if read_timeout is None else int(
        read_timeout.total_seconds())
    session_id = await execute_fetch_int(
        "EXECUTE FUNCTION cdc_opensess(?, 0, ?, ?, 1, 1);",
        (server, timeout, max_records),
    )

    for i, table in enumerate(tables):
        columns = cast(List[object], table.columns)
        column_placeholders = ", ".join("?" for _ in range(len(columns)))
        await execute_fetch_int(
            f"EXECUTE FUNCTION cdc_startcapture(?, 0, ?, ?, {column_placeholders}, ?);",
            [session_id, table] + columns + [i],
        ),

    await execute_fetch_int(
        "EXECUTE FUNCTION cdc_activatesess(?, ?);",
        (session_id, 0 if from_seq is None else from_seq),
    )

    async def generate():
        # await execute_fetch_int("{CALL ifx_lo_read(?, ?)", (session_id, buf))
        yield Record(123)

    return generate()


class CDCError(Exception):
    code: int
    name: str
    description: str

    def __init__(self, code: int, name: str, description: str) -> None:
        super().__init__(f"CDCError: {name} (code: {code}): {description}")
        self.code = code
        self.name = name
        self.description = description


class UnknownCDCErrorCode(Exception):
    code: int

    def __init__(self, code: int) -> None:
        super().__init__(f"Unknown CDC error code: {code}")


def _make_execute_fetch_int(cursor: Cursor, error_codes: Dict[int, CDCError]):
    async def execute_fetch_int(
            query: str,
            params: Optional[Sequence[Any]] = None,
    ) -> int:
        await cursor.execute(query, params)
        ret = await cursor.fetchval()
        assert isinstance(ret, int)
        _maybe_raise(ret, error_codes)
        return ret

    return execute_fetch_int


async def _load_error_codes(cursor: Cursor) -> Dict[int, CDCError]:
    error_codes: Dict[int, CDCError]
    await cursor.execute("SELECT * FROM syscdcerrcodes;")
    next_error_code = await cursor.fetchone()
    while next_error_code is not None:
        code = next_error_code.errcode
        assert isinstance(code, int)
        error_codes[code] = CDCError(
            code,
            next_error_code.errname,
            next_error_code.errdesc,
        )
        next_error_code = await cursor.fetchone()
    return error_codes


def _maybe_raise(maybe_error_code: int, codes: Dict[int, CDCError]) -> None:
    if maybe_error_code >= 0:  # Not an error code
        return
    error = codes.get(maybe_error_code, None)
    if error is not None:
        raise error
    raise UnknownCDCErrorCode(maybe_error_code)
