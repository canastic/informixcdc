package informixcdc

import com.informix.jdbcx.IfxDataSource
import java.math.BigDecimal
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.util.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class IntegrationTest {
    @Test
    fun testIntegration() {
        if (System.getenv("INFORMIXCDC_TEST_ENABLE") != "true") {
            println("Integration tests disabled; enable with env var INFORMIXCDC_TEST_ENABLE=true")
            return
        }

        useTestDatabase { conn ->
            testTables(
                conn,
                listOf(
                    listOf(
                        TestColumn("CHAR(1)") { i, stmt -> "x".also { stmt.setString(i, it) } },
                        TestColumn("CHAR(1)") { i, stmt -> null.also { stmt.setString(i, it) } }
                    ),
                    listOf(10, 20, 30).map { chars ->
                        TestColumn("CHAR($chars)") { i, stmt -> "x".repeat(chars).also { stmt.setString(i, it) } }
                    },
                    listOf(
                        TestColumn("SMALLINT") { i, stmt -> 13.toShort().also { stmt.setShort(i, it) } },
                        TestColumn("SMALLINT") { i, stmt -> (-13).toShort().also { stmt.setShort(i, it) } },
                        TestColumn("SMALLINT") { i, stmt -> Short.MAX_VALUE.also { stmt.setShort(i, it) } },
                        TestColumn("SMALLINT") { i, stmt -> (Short.MIN_VALUE + 1).toShort().also { stmt.setShort(i, it) } },
                        TestColumn("SMALLINT") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("INTEGER") { i, stmt -> 13.also { stmt.setInt(i, it) } },
                        TestColumn("INTEGER") { i, stmt -> (-13).also { stmt.setInt(i, it) } },
                        TestColumn("INTEGER") { i, stmt -> Int.MAX_VALUE.also { stmt.setInt(i, it) } },
                        TestColumn("INTEGER") { i, stmt -> (Int.MIN_VALUE + 1).also { stmt.setInt(i, it) } },
                        TestColumn("INTEGER") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("FLOAT") { i, stmt -> 12.34.also { stmt.setDouble(i, it) } },
                        TestColumn("FLOAT") { i, stmt -> (-12.34).also { stmt.setDouble(i, it) } },
                        TestColumn("FLOAT") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("SMALLFLOAT") { i, stmt -> 12.34F.also { stmt.setFloat(i, it) } },
                        TestColumn("SMALLFLOAT") { i, stmt -> (12.34F).also { stmt.setFloat(i, it) } },
                        TestColumn("SMALLFLOAT") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("DECIMAL") { i, stmt -> BigDecimal("12.34").also { stmt.setString(i, it.toString()) } },
                        TestColumn("DECIMAL") { i, stmt -> BigDecimal("-12.34").also { stmt.setString(i, it.toString()) } },
                        TestColumn("DECIMAL") { i, stmt -> null.also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("DATE") { i, stmt -> java.sql.Date(2017, 1, 10).also { stmt.setDate(i, it) } },
                        TestColumn("DATE") { i, stmt -> null.also { stmt.setDate(i, it) } }
                    ),
                    listOf(
                        TestColumn("MONEY") { i, stmt -> BigDecimal("12.34").also { stmt.setString(i, it.toString()) } },
                        TestColumn("MONEY") { i, stmt -> BigDecimal("-12.34").also { stmt.setString(i, it.toString()) } },
                        TestColumn("MONEY") { i, stmt -> null.also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("DATETIME HOUR TO SECOND") { i, stmt ->
                            stmt.setString(i, "22:44:13")
                            Timestamp(0, 1, 1, 22, 44, 13, 0)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME MONTH TO SECOND") { i, stmt ->
                            stmt.setString(i, "01-10 22:44:13")
                            Timestamp(0, 1, 10, 22, 44, 13, 0)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME YEAR TO SECOND") { i, stmt ->
                            stmt.setString(i, "2017-01-10 22:44:13")
                            Timestamp(2017, 1, 10, 22, 44, 13, 0)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME YEAR TO FRACTION(1)") { i, stmt ->
                            stmt.setString(i, "2017-01-10 22:44:13.5")
                            Timestamp(2017, 1, 10, 22, 44, 13, 500000000)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME YEAR TO FRACTION(2)") { i, stmt ->
                            stmt.setString(i, "2017-01-10 22:44:13.53")
                            Timestamp(2017, 1, 10, 22, 44, 13, 530000000)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME MINUTE TO FRACTION(5)") { i, stmt ->
                            stmt.setString(i, "44:13.53421")
                            Timestamp(0, 1, 1, 0, 44, 13, 534210000)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME MINUTE TO FRACTION(5)") { i, stmt ->
                            stmt.setString(i, "44:13.53421")
                            Timestamp(0, 1, 1, 0, 44, 13, 534210000)
                        }
                    ),
                    listOf(
                        TestColumn("DATETIME MINUTE TO FRACTION(5)") { i, stmt -> null.also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("NCHAR(4)") { i, stmt -> "TeSt".also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("INT8") { i, stmt -> 13L.also { stmt.setLong(i, it) } },
                        TestColumn("INT8") { i, stmt -> (-13L).also { stmt.setLong(i, it) } },
                        TestColumn("INT8") { i, stmt -> (Long.MAX_VALUE).also { stmt.setLong(i, it) } },
                        TestColumn("INT8") { i, stmt -> (Long.MIN_VALUE + 1).also { stmt.setLong(i, it) } },
                        TestColumn("INT8") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("BIGINT") { i, stmt -> 13L.also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> (-13L).also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> (Long.MAX_VALUE).also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> (Long.MIN_VALUE + 1).also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("BIGINT") { i, stmt -> 13L.also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> (-13L).also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> (Long.MAX_VALUE).also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> (Long.MIN_VALUE + 1).also { stmt.setLong(i, it) } },
                        TestColumn("BIGINT") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(10, 20).flatMap { prec ->
                        listOf(
                            TestColumn("DECIMAL($prec)") { i, stmt -> BigDecimal("12.34").also { stmt.setString(i, it.toString()) } },
                            TestColumn("DECIMAL($prec)") { i, stmt -> null.also { stmt.setString(i, it) } }
                        )
                    },
                    listOf(
                        TestColumn("VARCHAR(100)") { i, stmt -> "Please work".also { stmt.setString(i, it) } },
                        TestColumn("VARCHAR(100)") { i, stmt -> null.also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("NVARCHAR(100)") { i, stmt -> "Please work".also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("LVARCHAR(100)") { i, stmt -> "Please work".also { stmt.setString(i, it) } },
                        TestColumn("LVARCHAR(100)") { i, stmt -> null.also { stmt.setString(i, it) } }
                    ),
                    listOf(
                        TestColumn("BOOLEAN") { i, stmt -> true.also { stmt.setBoolean(i, it) } },
                        TestColumn("BOOLEAN") { i, stmt -> false.also { stmt.setBoolean(i, it) } },
                        TestColumn("BOOLEAN") { i, stmt -> null.also { stmt.setObject(i, it) } }
                    ),
                    listOf(
                        TestColumn("INT8") { i, stmt -> (-13L).also { stmt.setLong(i, it) } },
                        TestColumn("VARCHAR(20)") { i, stmt -> "Please work".also { stmt.setString(i, it) } },
                        TestColumn("DATETIME MINUTE TO FRACTION(5)") { i, stmt ->
                            stmt.setString(i, "44:13.53421")
                            Timestamp(0, 1, 1, 0, 44, 13, 534210000)
                        },
                        TestColumn("LVARCHAR") { i, stmt -> "".also { stmt.setString(i, it) } },
                        TestColumn("DECIMAL") { i, stmt -> BigDecimal("12.34").also { stmt.setString(i, it.toString()) } }
                    )
                ) + listOf(10, 15).flatMap { prec ->
                    (2..7).map { scale ->
                        listOf(
                            TestColumn("DECIMAL($prec, $scale)") { i, stmt -> BigDecimal("12.34").also { stmt.setString(i, it.toString()) } },
                            TestColumn("DECIMAL($prec, $scale)") { i, stmt -> null.also { stmt.setString(i, it) } }
                        )
                    }
                }
            )
        }
    }
}

fun testUser(): String = System.getenv("INFORMIXCDC_TEST_USER")

fun testServerName(): String = System.getenv("INFORMIXCDC_TEST_SERVERNAME")

fun getConn(database: String? = null): Connection =
    IfxDataSource().apply {
        ifxIFXHOST = System.getenv("INFORMIXCDC_TEST_HOST")
        portNumber = System.getenv("INFORMIXCDC_TEST_PORT").toInt()
        serverName = testServerName()
        user = testUser()
        password = System.getenv("INFORMIXCDC_TEST_PASSWORD")
        database?.let { databaseName = database }
    }.connection

fun <R> useTestDatabase(block: (TestConnection) -> R): R {
    val database = "informixcdc_test_${UUID.randomUUID().toString().replace("-", "")}"
    println("Creating database: $database")
    getConn().use { it.prepareStatement("CREATE DATABASE $database").execute() }

    val dropper = AutoCloseable {
        getConn().use { it.prepareStatement("DROP DATABASE $database").execute() }
        println("Dropped database: $database")
    }

    val logStopper = AutoCloseable {
        Runtime.getRuntime().exec(arrayOf("ontape", "-N", database)).waitFor()
    }

    return dropper.use {
        Runtime.getRuntime().exec(arrayOf("ontape", "-s", "-U", database)).waitFor()

        logStopper.use {
            getConn(database).use { conn ->
                block(TestConnection(conn, database))
            }
        }
    }
}

class TestConnection(conn: Connection, val database: String) : Connection by (conn)

data class TestColumn<T>(
    val type: String,
    val setAndReturnExpected: (Int, PreparedStatement) -> T
)

class TestTablesContext(conn: TestConnection, tables: List<List<TestColumn<*>>>) {
    val columnsByTable = HashMap<String, List<TestColumn<*>>>().let { columnsByTable ->
        for ((i, columns) in tables.withIndex()) {
            columnsByTable[tableName(i)] = columns
            testContext("table: ${tableName(i)} (${columnsByTable[tableName(i)]?.map { it.type }})") {
                createTable(conn, tableName(i), columns)
            }
        }
        columnsByTable
    }
}

fun testTables(conn: TestConnection, tables: List<List<TestColumn<*>>>) {
    val context = TestTablesContext(conn, tables)

    Records(
        { db -> getConn(db) },
        System.getenv("INFORMIXCDC_TEST_SERVERNAME"),
        tables.withIndex().map { (i, columns) ->
            TableDescription(
                tableName(i),
                conn.database,
                testUser(),
                ColumnNames(columns.withIndex().map { (i, _) -> columnName(i) })
            )
        }
    ).use { records ->
        getConn(conn.database).use { conn ->
            val iter = records.iterator()

            val expectedByTable = context.testInsert(conn, iter)
            context.testUpdate(conn, iter)
            context.testDelete(conn, iter, expectedByTable)
            context.testTruncate(conn, iter)

            // TODO: Serial
            // TODO: Rollback, discard
        }
    }
}

private fun TestTablesContext.testInsert(conn: Connection, records: Iterator<Record>): HashMap<String, ArrayList<Any?>> {
    val expectedByTable = doAndGetExpected(conn::insert)

    (0 until columnsByTable.count()).forEach {
        assertTrue(records.next() is BeginTx)

        assertExpected(records.next() as Insert, expectedByTable)

        assertTrue(records.next() is CommitTx)
    }

    return expectedByTable
}

private fun TestTablesContext.testUpdate(conn: Connection, records: Iterator<Record>) {
    val expectedByTable = doAndGetExpected(conn::update)

    (0 until columnsByTable.count()).forEach {
        assertTrue(records.next() is BeginTx)

        assertExpected(records.next() as BeforeUpdate, expectedByTable)
        assertExpected(records.next() as AfterUpdate, expectedByTable)

        assertTrue(records.next() is CommitTx)
    }
}

private fun TestTablesContext.testDelete(conn: Connection, records: Iterator<Record>, expectedByTable: HashMap<String, ArrayList<Any?>>) {
    for ((table, _) in expectedByTable) {
        conn.prepareStatement("DELETE FROM $table").use { it.execute() }
    }

    (0 until expectedByTable.count()).forEach {
        assertTrue(records.next() is BeginTx)

        assertExpected(records.next() as Delete, expectedByTable)

        assertTrue(records.next() is CommitTx)
    }
}

private fun TestTablesContext.testTruncate(conn: Connection, records: Iterator<Record>) {
    val expected = HashSet<String>()
    for ((table, _) in columnsByTable) {
        conn.prepareStatement("TRUNCATE TABLE $table").use { it.execute() }
        expected.add(table)
    }

    (0 until expected.count()).forEach {
        assertTrue(records.next() is BeginTx)

        val truncate = records.next() as Truncate
        assertTrue(expected.remove(truncate.table), "table: ${truncate.table}")

        assertTrue(records.next() is CommitTx)
    }

    assertTrue(expected.isEmpty(), "left: ${expected.toList()}")
}

data class ColumnSetter(val column: String, val setter: (Int, PreparedStatement) -> Unit)

private inline fun TestTablesContext.doAndGetExpected(
    crossinline op: (String, List<ColumnSetter>) -> Unit
): HashMap<String, ArrayList<Any?>> {
    val expectedByTable = HashMap<String, ArrayList<Any?>>()

    for ((table, columns) in columnsByTable) {
        val expected = ArrayList<Any?>().also { expectedByTable[table] = it }

        testContext("table: $table (${columnsByTable[table]?.map { it.type }})") {
            op(
                table,
                columns.withIndex().map { (i, column) ->
                    ColumnSetter(columnName(i)) { paramIdx, stmt ->
                        expected.add(column.setAndReturnExpected(paramIdx, stmt))
                    }
                }
            )
        }
    }
    return expectedByTable
}

private fun TestTablesContext.assertExpected(row: RowImage, expectedByTable: HashMap<String, ArrayList<Any?>>) {
    val columns = columnsByTable[row.table]!!

    testContext("table: ${row.table}") {
        val expectedRow = expectedByTable[row.table]!!
        val gotRow = row.values

        assertEquals(expectedRow.count(), gotRow.count())

        for ((i, expected) in expectedRow.withIndex()) {
            val colName = "c$i"
            testContext("column: $colName ${columns[i]}") {
                val got = gotRow[colName]!!
                assertEquals(expected, got.decode(), "column: $colName ${columns[i]}")
            }
        }
    }
}

fun tableName(i: Int): String = "table$i"
fun columnName(i: Int): String = "c$i"

fun createTable(conn: TestConnection, name: String, columns: List<TestColumn<*>>) {
    conn.prepareStatement(
        "CREATE TABLE $name (${
        columns.withIndex().map { (i, column) ->
            "${columnName(i)} ${column.type}"
        }.joinToString(", ")
        });"
    ).use {
        it.execute()
    }
}

fun Connection.insert(table: String, values: List<ColumnSetter>) {
    val placeholders = values.map { "?" }.joinToString(", ")
    val columnNames = values.map { (column, _) -> column }.joinToString(", ")
    prepareStatement("INSERT INTO $table ($columnNames) VALUES ($placeholders)").use { stmt ->
        for ((i, entry) in values.withIndex()) {
            val (_, setValue) = entry
            setValue(i + 1, stmt)
        }
        stmt.execute()
    }
}

fun Connection.update(table: String, values: List<ColumnSetter>) {
    val set = values.map { (column, _) -> "$column = ?" }.joinToString(", ")
    prepareStatement("UPDATE $table SET $set").use { stmt ->
        for ((i, entry) in values.withIndex()) {
            val (_, setValue) = entry
            setValue(i + 1, stmt)
        }
        stmt.execute()
    }
}

fun <R> testContext(context: String, block: () -> R) =
    try {
        block()
    } catch (e: Exception) {
        throw Exception(context, e)
    }
