package informixcdc.main

import com.beust.klaxon.Klaxon
import com.informix.jdbcx.IfxDataSource
import de.huxhorn.sulky.ulid.ULID
import informixcdc.InformixConnection
import informixcdc.Records
import informixcdc.RecordsMessage
import informixcdc.RecordsRequest
import informixcdc.TableDescription
import informixcdc.asInformix
import informixcdc.logx.duration
import informixcdc.logx.gauged
import informixcdc.logx.log
import informixcdc.logx.start
import informixcdc.main.config.heartbeatInterval
import informixcdc.main.config.request
import informixcdc.setUpConverters
import java.lang.Thread.interrupted
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

val klaxon = Klaxon().apply {
    RecordsRequest.setUpConverters(this)
    RecordsMessage.setUpConverters(this)
}

object config {
    val heartbeatInterval =
        System.getenv()["INFORMIXCDC_HEARTBEAT_INTERVAL"]?.let { Duration.parse(it) } ?: Duration.ofSeconds(5)
    val request =
        System.getenv("INFORMIXCDC_REQUEST").let { klaxon.parse<RecordsRequest>(it) }!!

    object informix {
        val host = System.getenv("INFORMIXCDC_INFORMIX_HOST")
        val port = System.getenv("INFORMIXCDC_INFORMIX_PORT").toInt()
        val serverName = System.getenv("INFORMIXCDC_INFORMIX_SERVER_NAME")
        val user = System.getenv("INFORMIXCDC_INFORMIX_USER")
        val password = System.getenv("INFORMIXCDC_INFORMIX_PASSWORD")
    }
}

fun main() {
    Records(
        getConn = ::getConn,
        server = config.informix.serverName,
        fromSeq = request.fromSeq,
        tables = request.tables.map {
            TableDescription(
                name = it.name,
                database = it.database,
                owner = it.owner,
                columns = it.columns
            )
        }
    ).use { records ->
        for (message in records.iterator().withHeartbeats(heartbeatInterval)) {
            println(klaxon.toJsonString(message))
        }
    }
}

internal sealed class TimedIteratorItem<out T>
internal data class Item<out T>(val item: T) : TimedIteratorItem<T>()
internal class Timeout<T> : TimedIteratorItem<T>()

internal fun <T, I : Iterator<T>> I.timed(timeout: Duration): Iterator<TimedIteratorItem<T>> = let { wrapped ->
    iterator {
        val done = AtomicBoolean(false)
        val items = LinkedBlockingQueue<TimedIteratorItem<T>>()

        val timer = thread(start = true) {
            while (!done.get()) {
                try {
                    sleep(timeout.toMillis())
                    items.put(Timeout())
                } catch (e: InterruptedException) {
                    interrupted()
                    continue
                }
            }
        }

        thread(start = true) {
            try {
                for (item in wrapped) {
                    timer.interrupt()
                    items.put(Item(item))
                }
            } finally {
                items.put(null)
            }
        }

        while (true) {
            val item = items.take()
            if (item != null) {
                yield(item)
            } else {
                break
            }
        }

        done.set(true)
    }
}

internal fun Iterator<RecordsMessage.Record.Record>.withHeartbeats(interval: Duration): Iterator<RecordsMessage> =
    iterator {
        timed(interval).forEach { timedItem ->
            when (timedItem) {
                is Item -> yield(RecordsMessage.Record(timedItem.item))
                is Timeout -> yield(RecordsMessage.Heartbeat())
            }
        }
    }

internal val informixConnections = log.gauge("informix_connections")

internal fun getConn(database: String): InformixConnection =
    with(
        IfxDataSource().apply {
            ifxIFXHOST = config.informix.host
            portNumber = config.informix.port
            serverName = config.informix.serverName
            user = config.informix.user
            password = config.informix.password
            databaseName = database
        }
    ) {
        val id = "informix_conn:${ULID().nextULID()}"

        val conn =
            log.duration(
                "informix_connected",
                "informix_address" to "${config.informix.host}:${config.informix.port}",
                "informix_database" to "$database@${config.informix.serverName}",
                "running_id" to id
            ) {
                getConnection()
            }

        val lifetime = gauged(informixConnections, log.start(id))
        object : InformixConnection by conn.asInformix() {
            override fun close() = try {
                lifetime.stopping()
                conn.close()
            } finally {
                lifetime.stopped()
            }
        }
    }
