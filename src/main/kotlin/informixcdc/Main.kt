package informixcdc

import com.beust.klaxon.Klaxon
import com.beust.klaxon.KlaxonException
import com.informix.jdbcx.IfxDataSource
import informixcdc.logx.duration
import informixcdc.logx.error
import informixcdc.logx.event
import informixcdc.logx.inheritLog
import informixcdc.logx.log
import informixcdc.logx.running
import informixcdc.logx.scope
import io.javalin.Javalin
import io.javalin.websocket.WsSession
import java.util.concurrent.ConcurrentHashMap

val parseRecordsRequest = with(Klaxon()) {
    RecordsRequest.setUpConverters(this)
    ({ msg: String -> parse<RecordsRequest>(msg)!! })
}

fun main() = main { shutdown, done ->
    val config = object {
        val serverPort = System.getenv("INFORMIXCDC_SERVER_PORT").toInt()
        val informix = object {
            val host = System.getenv("INFORMIXCDC_INFORMIX_HOST")
            val port = System.getenv("INFORMIXCDC_INFORMIX_PORT").toInt()
            val serverName = System.getenv("INFORMIXCDC_INFORMIX_SERVER_NAME")
            val user = System.getenv("INFORMIXCDC_INFORMIX_USER")
            val password = System.getenv("INFORMIXCDC_INFORMIX_PASSWORD")
        }
    }

    val getConn = { database: String ->
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
            log.duration("informix_connected") {
                connection
            }
        }
    }

    val app = Javalin.create()

    val threads = log.gauged(
        ConcurrentHashMap<String, RecordsForwarderThread>(),
        "records_forwarder_threads"
    ) { size.toLong() }
    val connections = log.gauge("websocket_connections")

    app.ws("/records") { ws ->
        ws.onConnect { session ->
            log.scope("records_forwarder") {
                log.event("connected", "session_id" to session.id, "remote_address" to session.remoteAddress)
                connections.add(1)
            }
        }

        ws.onMessage { session, msg ->
            log.scope("records_forwarder") {
                val request = try {
                    parseRecordsRequest(msg)
                } catch (e: KlaxonException) {
                    session.close(400, e.message!!)
                    return@scope
                }

                val records = Records(
                    getConn = getConn,
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
                val thread = RecordsForwarderThread(session, records)
                threads[session.id] = thread
                thread.start()
            }
        }

        ws.onClose { session, _, reason ->
            log.scope("records_forwarder") {
                try {
                    threads.remove(session.id)?.safeStop(reason?.let { Throwable(it) })
                } finally {
                    log.event(
                        "closed",
                        "session_id" to session.id,
                        "remote_address" to session.remoteAddress,
                        "reason" to (reason ?: "")
                    )
                    connections.add(-1)
                }
            }
        }
    }

    log.running("server", "server_port" to config.serverPort, stopping = shutdown) {
        app.start(config.serverPort)

        synchronized(shutdown) { shutdown.wait() }

        app.stop()
    }
}

fun main(block: (Object, Object) -> Unit) {
    val shutdown = Object()
    val done = Object()

    Runtime.getRuntime().addShutdownHook(
        Thread {
            synchronized(shutdown) { shutdown.notifyAll() }
            synchronized(done) { done.wait(10 * 1000) }
        }
    )

    try {
        log.running("main", stopping = shutdown) {
            block(shutdown, done)
        }
    } finally {
        synchronized(done) { done.notifyAll() }
    }
}

class RecordsForwarderThread(
    private val session: WsSession,
    private val records: Records
) : Thread() {
    private val inherit = inheritLog()
    private var error: Throwable? = null

    override fun run() = inherit {
        log.running(session.id) {
            val klaxon = Klaxon()

            records.use { records ->
                try {
                    for (record in records) {
                        log.event(
                            "record_received",
                            "record_seq" to record.seq,
                            "record_type" to record::class.simpleName!!
                        )
                        session.send(klaxon.toJsonString(record))
                    }
                } catch (e: Throwable) {
                    if (!interrupted()) {
                        session.close(500, "Internal Server Error")
                        throw e
                    }
                    error?.let { e ->
                        log.error(e)
                    }
                }
            }
        }
    }

    fun safeStop(t: Throwable? = null) {
        synchronized(this) {
            error = t
            interrupt()
        }
    }
}