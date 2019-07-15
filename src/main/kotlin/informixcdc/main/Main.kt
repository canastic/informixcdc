package informixcdc.main

import com.beust.klaxon.Klaxon
import com.beust.klaxon.KlaxonException
import com.informix.jdbcx.IfxDataSource
import de.huxhorn.sulky.ulid.ULID
import informixcdc.InformixConnection
import informixcdc.Records
import informixcdc.RecordsMessage
import informixcdc.RecordsRequest
import informixcdc.TableDescription
import informixcdc.asInformix
import informixcdc.logx.duration
import informixcdc.logx.error
import informixcdc.logx.event
import informixcdc.logx.gauged
import informixcdc.logx.inheritLog
import informixcdc.logx.log
import informixcdc.logx.running
import informixcdc.logx.scope
import informixcdc.logx.start
import informixcdc.logx.uncaught
import informixcdc.main.config.heartbeatInterval
import informixcdc.main.config.monitorAPIPort
import informixcdc.setUpConverters
import io.javalin.Javalin
import io.javalin.websocket.WsSession
import org.eclipse.jetty.server.Server
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

val klaxon = Klaxon().apply {
    RecordsRequest.setUpConverters(this)
    RecordsMessage.setUpConverters(this)
}

object config {
    val serverHost = System.getenv("INFORMIXCDC_SERVER_HOST").nonEmpty()
    val serverPort = System.getenv("INFORMIXCDC_SERVER_PORT").toInt()
    val heartbeatInterval =
        System.getenv()["INFORMIXCDC_HEARTBEAT_INTERVAL"]?.let { Duration.parse(it) } ?: Duration.ofSeconds(5)
    val monitorAPIPort = System.getenv()["MONITOR_API_PORT"]?.toInt()

    object informix {
        val host = System.getenv("INFORMIXCDC_INFORMIX_HOST")
        val port = System.getenv("INFORMIXCDC_INFORMIX_PORT").toInt()
        val serverName = System.getenv("INFORMIXCDC_INFORMIX_SERVER_NAME")
        val user = System.getenv("INFORMIXCDC_INFORMIX_USER")
        val password = System.getenv("INFORMIXCDC_INFORMIX_PASSWORD")
    }
}

fun main() = main { shutdown ->
    startMonitorAPI()

    val app = Javalin.create()

    val threads = log.gauged(
        ConcurrentHashMap<String, RecordsForwarderThread>(),
        "records_forwarder_threads"
    ) { size.toLong() }
    val connections = log.gauge("websocket_connections")

    app.ws("/records") { ws ->
        ws.onConnect { session ->
            recordsForwarderScope(session) {
                log.event("connected", "remote_address" to session.remoteAddress)
                connections.add(1)
            }
        }

        ws.onMessage { session, msg ->
            recordsForwarderScope(session) {
                val request = try {
                    klaxon.parse<RecordsRequest>(msg)!!
                } catch (e: KlaxonException) {
                    session.close(400, e.message!!)
                    return@recordsForwarderScope
                }

                if (threads.containsKey(session.id)) {
                    session.close(400, "already subscribed")
                    return@recordsForwarderScope
                }

                val records = Records(
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
                )
                val thread = log.uncaught(
                    RecordsForwarderThread(
                        RecordsForwarderWsSession(session),
                        records,
                        heartbeatInterval
                    )
                )
                threads[session.id] = thread
                thread.start()
            }
        }

        ws.onClose { session, _, reason ->
            recordsForwarderScope(session) {
                try {
                    threads.remove(session.id)?.safeStop(reason?.let { Throwable(it) })
                } finally {
                    log.event("closed", "reason" to (reason ?: ""))
                    connections.add(-1)
                }
            }
        }
    }

    running(
        log.start(
            "server",
            "server_host" to (config.serverHost ?: ""),
            "server_port" to config.serverPort
        ),
        stopping = shutdown
    ) {
        app.server {
            Server(
                if (config.serverHost != null) {
                    InetSocketAddress(config.serverHost, config.serverPort)
                } else {
                    InetSocketAddress(config.serverPort)
                }
            )
        }.start()

        synchronized(shutdown) { shutdown.wait() }

        app.stop()
    }
}

internal fun <T> recordsForwarderScope(session: WsSession, block: () -> T): T =
    log.scope("records_forwarder", "ws_id" to session.id) {
        block()
    }

internal class ObjectOf<T>(var value: T) : Object()

internal fun main(block: (Object) -> Unit) {
    val shutdown = Object()
    val done = ObjectOf(false)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            shutdown.sync { shutdown.notifyAll() }
            done.sync {
                wait(10 * 1000)
                if (!value) {
                    log.event("shutdown_timeout")
                }
            }
        }
    )

    try {
        running(log.start("main"), stopping = shutdown) {
            block(shutdown)
        }
    } finally {
        done.sync {
            value = true
            notifyAll()
        }
    }
}

internal class RecordsForwarderWsSession(
    private val session: WsSession
) {
    val id
        get() = session.id

    fun send(message: RecordsMessage) =
        session.send(klaxon.toJsonString(message))

    fun close(statusCode: Int, reason: String) =
        session.close(statusCode, reason)
}

internal class RecordsForwarderThread(
    private val session: RecordsForwarderWsSession,
    private val records: Records,
    private val heartbeatInterval: Duration
) : Thread() {
    private val inherit = inheritLog()
    private var error: Throwable? = null

    override fun run() = inherit {
        val done = AtomicBoolean(false)

        val heartbeater = log.uncaught(
            Thread {
                while (!done.get()) {
                    try {
                        sleep(heartbeatInterval.toMillis())
                    } catch (e: InterruptedException) {
                        continue
                    }

                    if (done.get()) {
                        break
                    }
                    session.sync { send(RecordsMessage.Heartbeat()) }
                }
            }
        ).apply { start() }

        running(log.start(session.id)) {
            try {
                records.use { records ->
                    for (record in records) {
                        log.event(
                            "record_received",
                            "record_seq" to record.seq,
                            "record_type" to record::class.simpleName!!
                        )
                        session.sync { send(RecordsMessage.Record(record)) }
                        heartbeater.interrupt()
                    }
                }
            } catch (e: Throwable) {
                if (!interrupted()) {
                    session.sync { close(500, "Internal Server Error") }
                    throw e
                }
                sync { error }?.let { e ->
                    log.error(e)
                }
            } finally {
                done.set(true)
                heartbeater.interrupt()
            }
        }
    }

    fun safeStop(t: Throwable? = null) {
        synchronized(this) {
            error = t
        }
        interrupt()
    }
}

internal fun <T : Any, R> T.sync(block: (T.() -> R)): R =
    synchronized(this) { block() }

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

private fun String.nonEmpty(): String? = when (this) {
    "" -> null
    else -> this
}

private fun startMonitorAPI() {
    monitorAPIPort?.let { port ->
        Thread {
            val app = Javalin.create()
            app.get("/health") { ctx ->
                ctx.result("\"ok\"")
            }
            app.server { Server(InetSocketAddress("127.0.0.1", port)) }
            app.start()
        }.start()
    }
}
