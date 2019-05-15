package informixcdc

import com.informix.jdbc.IfxConnection
import com.informix.jdbc.IfxSmartBlob
import com.informix.lang.IfxToJavaType
import com.informix.lang.IfxTypes
import com.informix.lang.IfxTypes.IFX_TYPE_BIGINT
import com.informix.lang.IfxTypes.IFX_TYPE_BIGSERIAL
import com.informix.lang.IfxTypes.IFX_TYPE_DATETIME
import com.informix.lang.IfxTypes.IFX_TYPE_DECIMAL
import com.informix.lang.IfxTypes.IFX_TYPE_INT8
import com.informix.lang.IfxTypes.IFX_TYPE_INTERVAL
import com.informix.lang.IfxTypes.IFX_TYPE_LVARCHAR
import com.informix.lang.IfxTypes.IFX_TYPE_MONEY
import com.informix.lang.IfxTypes.IFX_TYPE_NVCHAR
import com.informix.lang.IfxTypes.IFX_TYPE_UDTFIXED
import com.informix.lang.IfxTypes.IFX_TYPE_UDTVAR
import com.informix.lang.IfxTypes.IFX_TYPE_VARCHAR
import informixcdc.RecordsMessage.Record.Record
import informixcdc.RecordsMessage.Record.Record.RowImage.AfterUpdate
import informixcdc.RecordsMessage.Record.Record.RowImage.BeforeUpdate
import informixcdc.RecordsMessage.Record.Record.RowImage.Delete
import informixcdc.RecordsMessage.Record.Record.RowImage.Insert
import java.lang.Thread.currentThread
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.Duration
import java.time.Instant
import java.util.ArrayList
import java.util.Base64
import java.util.HashMap

internal data class CDCError(
    val code: Int,
    val name: String,
    val description: String
)

class CDCException(
    val code: Int,
    val name: String,
    val description: String
) : Exception("CDCError: $name (code: $code): $description") {
    internal constructor(e: CDCError) : this(e.code, e.name, e.description)
}

class UnknownCDCErrorCode(val code: Int) : Exception("Unknown CDC error code: $code")

class TableDescription(
    val name: String,
    val database: String,
    val owner: String,
    val columns: List<String>? = null
)

private data class FullColumnsDescription(
    val fixed: List<Pair<ColumnWithDecoder, Int>>,
    val variable: List<ColumnWithDecoder>
)

private fun FullColumnsDescription.fixedThenVariable(): List<String> =
    fixed.map { (nameType, _) -> nameType.name } + variable.map { it.name }

class Records(
    private val getConn: (String) -> InformixConnection,
    private val server: String,
    private val tables: List<TableDescription>,
    private val fromSeq: Long? = null,
    private val readTimeout: Duration? = Duration.ofMillis(1 * 1000),
    private val maxRecords: Long = 100
) {
    fun <R> use(block: (Iterable<Record>) -> R): R {
        val sizedTables = loadSizes(getConn, tables)

        getConn("syscdcv1").use { conn ->
            val errorCodes = conn.loadErrorCodes()
            val recordTypes = conn.loadRecordTypes()
            val getCDCResult = conn.makeGetCDCResult(errorCodes)

            val sessionID: Long = conn.fetchOne("EXECUTE FUNCTION cdc_opensess(?, 0, ?, ?, 1, 1);") { row ->
                setString(1, server)
                setLong(2, readTimeout?.seconds ?: -1)
                setLong(3, maxRecords)
                row {
                    getLong(1)
                }
            }

            val tablesByID = hashMapOf<Int, SizedTable>()

            for (table in sizedTables) {
                try {
                    conn.getCDCResult("EXECUTE FUNCTION cdc_set_fullrowlogging(?, ?);") {
                        setString(1, table.fullName())
                        setInt(2, 1)
                    }

                    conn.getCDCResult("EXECUTE FUNCTION cdc_startcapture(?, 0, ?, ?, ?);") {
                        setLong(1, sessionID)
                        setString(2, table.fullName())
                        setString(3, table.columns.fixedThenVariable().joinToString(separator = ","))
                        setInt(4, table.id)
                    }

                    tablesByID[table.id] = table
                } catch (e: Throwable) {
                    throw Throwable("couldn't start capturing table ${table.name}", e)
                }
            }

            conn.getCDCResult("EXECUTE FUNCTION cdc_activatesess(?, ?);") {
                setLong(1, sessionID)
                setLong(2, fromSeq ?: 0)
            }

            return try {
                block(
                    RecordsIterable(
                        conn.recordBytes(sessionID),
                        recordTypes,
                        errorCodes,
                        tablesByID
                    )
                )
            } finally {
                conn.getCDCResult("EXECUTE FUNCTION cdc_closesess(?);") {
                    setLong(1, sessionID)
                }
            }
        }
    }
}

interface InformixConnection : Connection {
    val informix: IfxConnection
}

fun Connection.asInformix(): InformixConnection = let { conn ->
    object : Connection by conn, InformixConnection {
        override val informix = conn as IfxConnection
    }
}

private data class SizedTable(
    val id: Int,
    val name: String,
    val database: String,
    val owner: String?,
    var columns: FullColumnsDescription
)

private fun loadSizes(getConn: (String) -> Connection, tables: List<TableDescription>): Array<SizedTable> {
    val tablesArray = tables.toTypedArray()

    return Array(tablesArray.count()) { i ->
        val table = tablesArray[i]
        getConn(table.database).use { conn ->
            val (id, columns) = conn.loadTableSize(table.name, table.columns)
            SizedTable(id, table.name, table.database, table.owner, columns)
        }
    }
}

private fun SizedTable.fullName(): String =
    "$database:$owner.$name"

private fun Connection.fetchTableID(table: String): Int {
    return fetchOne("SELECT tabid FROM systables WHERE tabname = ?;") { row ->
        setString(1, table)
        row {
            getInt(1)
        }
    }
}

data class ColumnWithDecoder(
    val name: String,
    val decode: (ByteArray) -> Any?
)

private fun Connection.loadTableSize(table: String, columns: List<String>? = null): Pair<Int, FullColumnsDescription> {
    val tableID = fetchTableID(table)

    val fixed = ArrayList<Pair<ColumnWithDecoder, Int>>()
    val variable = ArrayList<ColumnWithDecoder>()

    val andCols = columns?.run {
        "AND (${map { "colname = ?" }.joinToString(separator = " OR ")})"
    } ?: ""
    query("SELECT colname, coltype, collength, xt.name FROM syscolumns sc LEFT JOIN sysxtdtypes xt ON sc.extended_id = xt.extended_id WHERE tabid = ? $andCols;") { row ->
        setInt(1, tableID)
        columns?.withIndex()?.forEach { (i, column) -> setString(i + 2, column) }
        row {
            val name = getString(1)
            val type = getInt(2) and 0xFF // The other bytes are metadata.
            val length = getInt(3)
            val typeName: String? = getString(4)

            val desc = ColumnWithDecoder(name, decoderForType(type, typeName, length))

            when (type.toShort()) {
                IFX_TYPE_VARCHAR,
                IFX_TYPE_NVCHAR,
                IFX_TYPE_UDTVAR,
                IFX_TYPE_LVARCHAR
                ->
                    variable.add(desc)
                else ->
                    fixed.add(Pair(desc, columnSizeForType(type, typeName, length)))
            }
        }
    }

    return Pair(tableID, FullColumnsDescription(fixed, variable))
}

private fun Connection.loadErrorCodes(): Map<Int, CDCError> {
    val codes = hashMapOf<Int, CDCError>()
    query("SELECT errcode, errname, errdesc FROM syscdcerrcodes;") { row ->
        row {
            codes[getInt(1)] = CDCError(getInt(1), getString(2), getString(3))
        }
    }
    return codes
}

private fun Connection.loadRecordTypes(): Map<Int, String> {
    val types = hashMapOf<Int, String>()
    query("SELECT recnum, recname FROM syscdcrectypes;") { row ->
        row {
            types[getInt(1)] = getString(2)
        }
    }
    return types
}

private fun Connection.makeGetCDCResult(errorCodes: Map<Int, CDCError>): (Connection).(String, PreparedStatement.() -> Unit) -> Int =
    { sql, config ->
        fetchOne(sql) { row ->
            config(this)
            row {
                errorCodes.maybeThrow(getInt(1))
            }
        }
    }

private fun Map<Int, CDCError>.maybeThrow(maybeErrorCode: Int): Int {
    if (maybeErrorCode >= 0) { // Not an error code.
        return maybeErrorCode
    }
    val error = get(maybeErrorCode)
    if (error != null) {
        throw CDCException(error)
    }
    throw UnknownCDCErrorCode(maybeErrorCode)
}

private fun Connection.query(sql: String, config: PreparedStatement.((ResultSet.() -> Unit) -> Unit) -> Unit) {
    prepareStatement(sql).use { stmt ->
        config(stmt) { handleRow ->
            stmt.executeQuery().use { rows ->
                while (rows.next()) {
                    handleRow(rows)
                }
            }
        }
    }
}

private fun <T> Connection.fetchOne(sql: String, config: PreparedStatement.((ResultSet.() -> T) -> Unit) -> Unit): T {
    var result: T? = null
    query(sql) { row ->
        config(this) { handleRow ->
            row {
                if (result != null) {
                    throw RuntimeException("got more than one result.")
                }
                result = handleRow(this)
            }
        }
    }
    return result!!
}

private fun InformixConnection.recordBytes(sessionID: Long): Iterable<Byte> {
    val buffer = ByteArray(4096)
    val blob = IfxSmartBlob(this.informix)

    return Iterable {
        iterator {
            // TODO: Is this really the best way to handle cancellation?
            while (!currentThread().isInterrupted) {
                val buffered = blob.IfxLoRead(sessionID.toInt(), buffer, buffer.count())
                yieldAll(buffer.take(buffered))
            }
        }
    }
}

private class RecordsIterable(
    val bytes: Iterable<Byte>,
    val types: Map<Int, String>,
    val errorCodes: Map<Int, CDCError>,
    val tablesByID: Map<Int, SizedTable>
) : Iterable<Record> {
    private var bytesIter = bytes.iterator()

    init {
        // Force first record; it should be TABSCHEMA.
        assert(next(bytesIter) == null)
    }

    override fun iterator(): Iterator<Record> = iterator {
        while (true) {
            try {
                while (true) {
                    next(bytesIter)?.let { yield(it) }
                }
            } catch (_: CDCTimeout) {
                // Just reset iterator.
                bytesIter = bytes.iterator()
            }
        }
    }
}

internal class CDCTimeout : Throwable("timeout")

@Throws(CDCTimeout::class)
private fun RecordsIterable.next(bytes: Iterator<Byte>): Record? {
    bytes.drop(4) // Header size
    val payloadSize = bytes.readInt()
    bytes.drop(4) // Packet scheme
    val recordNumber = bytes.readInt()

    return when (types[recordNumber]) {
        "BEGINTX" ->
            Record.BeginTx(
                bytes.readLong(),
                bytes.readInt().toLong(),
                Instant.ofEpochSecond(bytes.readLong()).toString(),
                bytes.readInt().toLong()
            )
        "COMMTX" ->
            Record.CommitTx(
                bytes.readLong(),
                bytes.readInt().toLong(),
                Instant.ofEpochSecond(bytes.readLong()).toString()
            )
        "RBTX" ->
            Record.RollbackTx(
                bytes.readLong(),
                bytes.readInt().toLong()
            )
        "INSERT" -> {
            decodeRowImage(bytes, payloadSize, ::Insert)
        }
        "DELETE" ->
            decodeRowImage(bytes, payloadSize, ::Delete)
        "UPDBEF" ->
            decodeRowImage(bytes, payloadSize, ::BeforeUpdate)
        "UPDAFT" ->
            decodeRowImage(bytes, payloadSize, ::AfterUpdate)
        "DISCARD" ->
            Record.Discard(
                bytes.readLong(),
                bytes.readInt().toLong()
            )
        "TRUNCATE" ->
            decodeTableIDHeader(bytes).let { (seq, txID, table) ->
                Record.Truncate(seq, txID.toLong(), table.name, table.database, table.owner)
            }
        "TABSCHEMA" -> {
            val tableID = bytes.readInt()
            bytes.drop(4) // Flags
            bytes.drop(4) // Fixed-length size
            val numFixed = bytes.readInt()
            bytes.drop(4) // Variable-length columns

            // We're only interested in the column order that TABSCHEMA reports, because that's the order in which
            // other records will bring column values. So we rearrange the table entry in tablesByID.
            //
            // > Names of any fixed-length columns appear before names of any variable-length columns.

            val orderedColumns =
                bytes
                    .take(payloadSize)
                    .toString(Charset.forName("UTF-8"))
                    .split(", ")
                    .map { it.split(" ", limit = 1)[0] } // That should be the name
            val fixedOrdered = orderedColumns.take(numFixed)
            val variableOrdered = orderedColumns.drop(numFixed)

            val table =
                tablesByID[tableID]
                    ?: throw RuntimeException("got TABSCHEMA record for unexpected table ID: $tableID")

            // Rearrange columns in table entry by sorting them by their position in the corresponding ordered column
            // list.
            table.columns = FullColumnsDescription(
                table.columns.fixed.sortedBy { (it, _) -> fixedOrdered.indexOf(it.name) },
                table.columns.variable.sortedBy { variableOrdered.indexOf(it.name) }
            )

            null
        }
        "TIMEOUT" -> {
            throw CDCTimeout()
        }
        "ERROR" -> {
            drop(4)
            val code = bytes.readInt()
            errorCodes.maybeThrow(code)
            throw UnknownCDCErrorCode(code)
        }
        else -> {
            throw RuntimeException("Unknown record type number: $recordNumber")
        }
    }
}

private fun RecordsIterable.decodeRowImage(
    bytes: Iterator<Byte>,
    payloadSize: Int,
    constructor: (Long, Long, String, String, String?, Map<String, Record.RowImage.ColumnValue>) -> Record.RowImage
): Record.RowImage {
    val (seq, txID, table) = decodeTableIDHeader(bytes)

    bytes.drop(4) // Reserved flags.

    val varLengths = Array(table.columns.variable.count()) {
        bytes.readInt()
    }

    val values = HashMap<String, Record.RowImage.ColumnValue>()

    var took = 0
    for ((column, size) in table.columns.fixed + table.columns.variable.zip(varLengths)) {
        val raw = bytes.take(size)
        values[column.name] = Record.RowImage.ColumnValue(
            Base64.getEncoder().encode(raw).toString(UTF_8),
            column.decode(raw)
        )
        took += size
    }

    assert(payloadSize - took == 0) {
        "Expected payload of size $took, but record header reports $payloadSize for table ${table.fullName()}"
    }

    return constructor(seq, txID.toLong(), table.name, table.database, table.owner, values)
}

private data class TableIDHeader(val seq: Long, val txID: Int, val table: SizedTable)

private fun RecordsIterable.decodeTableIDHeader(bytes: Iterator<Byte>): TableIDHeader = TableIDHeader(
    bytes.readLong(),
    bytes.readInt(),
    bytes.readInt().let { tableID ->
        tablesByID[tableID]
            ?: throw RuntimeException("retrieved record for unknown table ID $tableID")
    }
)

private fun Iterator<Byte>.readInt(): Int =
    byteArrayOf(next(), next(), next(), next()).let { ByteBuffer.wrap(it).order(ByteOrder.BIG_ENDIAN).int }

private fun Iterator<Byte>.readLong(): Long =
    byteArrayOf(next(), next(), next(), next(), next(), next(), next(), next()).let {
        ByteBuffer.wrap(it).order(ByteOrder.BIG_ENDIAN).long
    }

private fun <T> Iterator<T>.drop(n: Int) {
    for (i in 0 until n) {
        next()
    }
}

private fun Iterator<Byte>.take(n: Int): ByteArray {
    val ret = ByteArray(n)
    for (i in 0 until n) {
        ret[i] = next()
    }
    return ret
}

private fun decoderForType(type: Int, name: String?, length: Int): (ByteArray) -> Any? =
    when (type.toShort()) {
        IFX_TYPE_DATETIME ->
            // IfxTypes.FromIfxTypeToJava doesn't seem to work for this one!
            dateTimeDecoder(length)
        IFX_TYPE_BIGINT,
        IFX_TYPE_BIGSERIAL -> { raw ->
            when {
                raw.isNullInt() -> null
                else -> IfxToJavaType.IfxToJavaLongBigInt(raw)
            }
        }
        IFX_TYPE_INT8 -> { raw ->
            when {
                Pair(raw[0], raw[1]) == Pair(0.toByte(), 0.toByte()) -> null
                else -> IfxToJavaType.IfxToJavaLongInt(raw)
            }
        }
        IFX_TYPE_VARCHAR,
        IFX_TYPE_NVCHAR -> { raw ->
            // First byte is length.
            when {
                Pair(raw[0], raw[1]) == Pair(1.toByte(), 0.toByte()) -> null
                else -> IfxToJavaType.IfxToJavaChar(raw, 1, raw.count() - 1, null, false)
            }
        }
        IFX_TYPE_UDTVAR -> when (name) {
            "lvarchar" -> { raw ->
                when {
                    Pair(raw[1], raw[2]) == Pair(1.toByte(), 1.toByte()) -> null
                    // First bytes: [0, length, 0]
                    else -> IfxToJavaType.IfxToJavaChar(raw, 3, raw.count() - 3, null, false)
                }
            }
            else ->
                { raw -> raw }
        }
        IFX_TYPE_UDTFIXED -> when (name) {
            "boolean" -> { raw ->
                when {
                    raw[0] == 1.toByte() -> null
                    else -> IfxToJavaType.IfxToJavaSmallInt(raw) == 1.toShort()
                }
            }
            else ->
                { raw -> raw }
        }
        else ->
            when (IfxTypes.FromIfxTypeToJava(type)) {
                "com.informix.lang.Interval" -> { raw ->
                    IfxToJavaType.IfxToJavaInterval(raw, (length shr 8).toShort())
                }
                "java.lang.Double" -> { raw ->
                    when {
                        raw.all { it == 0xFF.toByte() } -> null
                        else -> IfxToJavaType.IfxToJavaDouble(raw)
                    }
                }
                "java.lang.Float" -> { raw ->
                    when {
                        raw.all { it == 0xFF.toByte() } -> null
                        else -> IfxToJavaType.IfxToJavaReal(raw)
                    }
                }
                "java.lang.Integer" -> { raw ->
                    when {
                        raw.isNullInt() -> null
                        else -> IfxToJavaType.IfxToJavaInt(raw)
                    }
                }
                "java.lang.Short" -> { raw ->
                    when {
                        raw.isNullInt() -> null
                        else -> IfxToJavaType.IfxToJavaSmallInt(raw)
                    }
                }
                "java.lang.String" -> { raw ->
                    when {
                        raw[0] == 0.toByte() -> null
                        else -> IfxToJavaType().IfxToJavaChar(raw, false)
                    }
                }
                "java.math.BigDecimal" -> { raw ->
                    when {
                        raw[0] == 0.toByte() -> null
                        else -> IfxToJavaType.IfxToJavaDecimal(raw, raw.size.toShort())
                    }
                }
                "java.sql.Date" -> { raw ->
                    IfxToJavaType.IfxToJavaDate(raw)
                }
                else -> { raw -> raw }
            }
    }

private fun ByteArray.isNullInt(): Boolean =
    this[0] == (-128).toByte() && this.drop(1).all { it == 0.toByte() }

private fun dateTimeDecoder(collengthFromSyscolumns: Int): (ByteArray) -> java.sql.Timestamp? {
    // A date like
    //
    //     01-10T22:44:13.100
    //
    // comes from Informix like this:
    //
    //     [-57, 1, 10, 22, 44, 13, 10]
    //
    // From the second byte onwards, we find a textual representation in which each byte holds two _characters_. (The
    // first byte seems to be relevant only to check for null.)
    //
    // The series starts at the granularity (year, month, etc.) encoded in the collength column from the syscolumns
    // table, and stops at some other granularity. Since LocalDateTime.of takes an argument per level of granularity, we
    // need to fill with zeros both sides until we've got values from years to nanoseconds, like this:
    //
    //     [0, 0, 1, 10, 22, 44, 13, 10, 00, 00]
    //
    // Then, we just map each entry in this array to an argument to LocalDate.of.

    // Let's first decode how many zeros we should put before the payload.
    val leadingZeros = (collengthFromSyscolumns shr 4 and 0xF).let {
        var halfBytes = it
        if (halfBytes % 2 == 1) {
            halfBytes += 1 // If odd, round up to nearest even, so that it cleanly divides by bytes.
        }
        var bytes = halfBytes / 2
        if (bytes > 0) {
            bytes += 1 // Years take two positions
        }
        bytes
    }

    return fun(raw: ByteArray): java.sql.Timestamp? {
        if (raw[0] == 0.toByte()) {
            return null
        }

        val bytesFromRaw = raw.size - 1

        val components = Array(10) { i ->
            when {
                i >= leadingZeros && i < leadingZeros + bytesFromRaw ->
                    raw[i - leadingZeros + 1].toInt()
                else ->
                    0
            }
        }.iterator()
        return java.sql.Timestamp(
            components.next() * 100 + components.next(),
            components.next().let {
                when (it) {
                    0 -> 1 // Default: January
                    else -> it
                }
            },
            components.next().let {
                when (it) {
                    0 -> 1 // No day 0
                    else -> it
                }
            },
            components.next(),
            components.next(),
            components.next(),
            components.next() * 1000 * 100 * 100 + components.next() * 1000 * 100 + components.next() * 1000
        )
    }
}

private fun columnSizeForType(type: Int, typeName: String?, length: Int): Int =
    when (type.toShort()) {
        IFX_TYPE_DATETIME,
        IFX_TYPE_INTERVAL
        ->
            // First 8 bits encode qualifiers which we don't care about.
            (length shr 8) / 2 + (length shr 8) % 2 + 1
        IFX_TYPE_DECIMAL,
        IFX_TYPE_MONEY
        -> {
            val scale = (length and 0xFF)
            val isScaled = scale != 255
            val digits = length shr 8
            digits / 2 + // 2 bytes per character
                digits % 2 +
                1 + // Sign?
                (if (!isScaled || (digits % 2 == 0 && scale % 2 != 0)) 1 else 0) // ??????
        }
        IFX_TYPE_UDTFIXED -> when (typeName) {
            "boolean" -> 2
            else -> length
        }
        else ->
            length
    }
