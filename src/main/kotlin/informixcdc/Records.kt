package informixcdc

import java.time.Instant

sealed class Record

data class BeginTx(
    val seq: Long,
    val transactionID: Int,
    val startAt: Instant,
    val userID: Int
) : Record()

data class CommitTx(
    val seq: Long,
    val transactionID: Int,
    val startAt: Instant
) : Record()

data class RollbackTx(
    val seq: Long,
    val transactionID: Int
) : Record()

sealed class RowImage(
    val seq: Long,
    val transactionID: Int,
    val table: String,
    val database: String,
    val owner: String?,
    val values: Map<String, ColumnValue>
) : Record()

class Insert(
    seq: Long,
    transactionID: Int,
    table: String,
    database: String,
    owner: String?,
    values: Map<String, ColumnValue>
) : RowImage(seq, transactionID, table, database, owner, values)

class Delete(
    seq: Long,
    transactionID: Int,
    table: String,
    database: String,
    owner: String?,
    values: Map<String, ColumnValue>
) : RowImage(seq, transactionID, table, database, owner, values)

class BeforeUpdate(
    seq: Long,
    transactionID: Int,
    table: String,
    database: String,
    owner: String?,
    values: Map<String, ColumnValue>
) : RowImage(seq, transactionID, table, database, owner, values)

class AfterUpdate(
    seq: Long,
    transactionID: Int,
    table: String,
    database: String,
    owner: String?,
    values: Map<String, ColumnValue>
) : RowImage(seq, transactionID, table, database, owner, values)

data class Discard(
    val seq: Long,
    val transactionID: Int
) : Record()

data class Truncate(
    val seq: Long,
    val transactionID: Int,
    val table: String,
    val database: String,
    val owner: String?
) : Record()
