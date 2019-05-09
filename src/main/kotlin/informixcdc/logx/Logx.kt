package informixcdc.logx

import com.beust.klaxon.Klaxon
import de.huxhorn.sulky.ulid.ULID
import java.lang.Thread.sleep
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread

val log: Log = Log(::emit)

val processID = ULID().nextULID()

private val serial = AtomicLong(0)
private val values = ThreadLocal.withInitial<MutableList<Pair<String, Any>>> {
    arrayListOf("process_id" to processID)
}

class Log internal constructor(
    private val emit: (Sequence<KV>) -> Unit
) {
    fun <T> add(vararg kvs: KV, block: () -> T): T {
        val prevLength = values.get().size

        return AutoCloseable {
            values.set(values.get().subList(0, prevLength))
        }.use {
            values.get().addAll(kvs)
            block()
        }
    }

    internal fun emit() {
        add(
            "t" to Instant.now(),
            "event_id" to "$processID:${serial.addAndGet(1)}"
        ) {
            emit(values.get().asSequence())
        }
    }

    private val gauges = ArrayList<GaugePoll>()

    fun gauge(name: String): Gauge =
        // TODO: Support tags.
        with(Gauge()) { gauged(this, name, { poll() }) }

    fun <T> gauged(v: T, name: String, poll: T.() -> Long): T =
        v.also {
            synchronized(gauges) { gauges.add(GaugePoll(name) { v.poll() }) }
        }

    init {
        thread(start = true, name = "gauges") {
            while (true) {
                sleep(5 * 1000)
                synchronized(gauges) {
                    for (gauge in gauges) {
                        add(
                            "metric_type" to "gauge",
                            "metric_name" to gauge.name,
                            "value" to gauge.poll()
                        ) {
                            emit()
                        }
                    }
                }
            }
        }
    }
}

fun Log.event(type: String, vararg kvs: KV) {
    add(*kvs) { add("event" to type) { emit() } }
}

fun Log.message(msg: String, vararg kvs: KV) {
    add(*kvs) { event("message", "msg" to msg) }
}

fun <T> Log.catch(vararg classes: Class<Throwable>, block: () -> T): T? =
    try {
        block()
    } catch (t: Throwable) {
        if (!classes.isEmpty() && !classes.any { it.isInstance(t) }) {
            log.error(t)
            null
        } else {
            throw t
        }
    }

fun Log.error(t: Throwable) {
    event(
        "error",
        *sequence {
            var e: Throwable? = t
            while (e != null) {
                yield("error" to e.message)
                yield("stack_trace" to e.stackTrace.toString())
                for (frame in e.stackTrace) {
                    yield("func" to "${frame.className}.${frame.methodName}")
                    yield("file" to frame.fileName)
                    yield("line" to "${frame.fileName}:${frame.lineNumber}")
                }
                e = e.cause
            }
        }.toList().toTypedArray()
    )
}

fun <T> Log.failable(successEvent: String, vararg classes: Class<Throwable>, block: () -> T): T? =
    catch(*classes) {
        val r = block()
        event(successEvent)
        r
    }

fun <T> Log.scope(name: String, vararg addKVs: KV, block: () -> T): T =
    add("scope" to name, *addKVs) { block() }

fun <T> running(lifetime: RunningLifetimeLogs, stopping: Object? = null, block: () -> T): T {
    stopping?.let { stopping ->
        val inherit = inheritLog()
        thread(start = true) {
            inherit {
                synchronized(stopping) { stopping.wait() }
                lifetime.stopping()
            }
        }
    }
    return try {
        block()
    } finally {
        lifetime.stopped()
    }
}

interface RunningLifetimeLogs {
    fun stopping()
    fun stopped()
}

fun Log.start(runningID: String, vararg addKVs: KV): RunningLifetimeLogs {
    val started = Instant.now()
    event("started", *addKVs, "running_id" to runningID)
    return object : RunningLifetimeLogs {
        override fun stopping() {
            event("stopping", "running_id" to runningID)
        }

        override fun stopped() {
            durationSince(started, "stopped", "running_id" to runningID)
        }
    }
}

fun gauged(gauge: Gauge, lifetime: RunningLifetimeLogs): RunningLifetimeLogs {
    gauge.add(1)
    return object : RunningLifetimeLogs {
        override fun stopping() {
            lifetime.stopping()
        }

        override fun stopped() {
            gauge.add(-1)
            lifetime.stopped()
        }
    }
}

fun Log.durationSince(started: Instant, eventType: String, vararg addKVs: KV) {
    event(
        eventType,
        "duration" to Duration.between(started, Instant.now()),
        *addKVs
    )
}

fun <T> Log.duration(eventType: String, vararg eventKVs: KV, block: () -> T): T =
    Instant.now().let { started ->
        try {
            block()
        } finally {
            durationSince(started, eventType, *eventKVs)
        }
    }

fun inheritLog(): (() -> Unit) -> Unit {
    val parentValues = ArrayList(values.get())
    return { block ->
        values.set(parentValues)
        block()
    }
}

class Gauge {
    private val value = AtomicLong(0)

    fun add(delta: Long) = value.addAndGet(delta)
    fun set(v: Long) = value.set(v)

    internal fun poll(): Long = value.get()
}

private class GaugePoll(
    val name: String,
    val poll: () -> Long
)
typealias KV = Pair<String, Any>

private fun emit(kvs: Sequence<KV>) {
    System.err.println(
        Klaxon().toJsonString(
            kvs
                .map { (k, v) -> listOf(k, v) }
                .toList()
        )
    )
}
