// Autogenerated by jsonschema2klaxon.py. DO NOT EDIT.

package informixcdc

import com.beust.klaxon.Json
import com.beust.klaxon.Klaxon
import com.beust.klaxon.Converter

private val converters = ArrayList<(Klaxon) -> Converter>()

class RecordsRequest(
    @Json(name = "from_seq")
    val fromSeq: Long?,

    @Json(name = "tables")
    val tables: List<TablesElement>
) {

    class TablesElement(
        @Json(name = "name")
        val name: String,

        @Json(name = "database")
        val database: String,

        @Json(name = "owner")
        val owner: String,

        @Json(name = "columns")
        val columns: List<String>?
    ) {

        companion object
    }

    companion object
}

fun RecordsRequest.Companion.setUpConverters(klaxon: Klaxon) {
    converters.forEach { klaxon.converter(it(klaxon)) }
}
