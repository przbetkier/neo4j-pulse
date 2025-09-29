package dev.przbetkier.collectors

import org.neo4j.driver.Record
import org.neo4j.driver.Session
import org.slf4j.LoggerFactory

class StoreMetricsCollector : MetricCollector {

    private val logger = LoggerFactory.getLogger(StoreMetricsCollector::class.java)

    override fun collect(session: Session): List<String> {
        val metrics = mutableListOf<String>()
        try {
            val storeResult = session.run("CALL apoc.monitor.store()")
            if (storeResult.hasNext()) {
                val storeRecord = storeResult.single()
                val storeMetrics = convertStoreMetricsToPrometheus(storeRecord)
                metrics.addAll(storeMetrics)
            }
        } catch (e: Exception) {
            logger.error("Error collecting store metrics (APOC may not be available): ${e.message}")
        }
        return metrics.toList()
    }

    private fun convertStoreMetricsToPrometheus(record: Record): List<String> {
        val metrics = mutableListOf<String>()

        try {
            // Common store metrics from apoc.monitor.store()
            val keys = record.keys()

            keys.forEach { key ->
                val value = record.get(key)
                when (key.lowercase()) {
                    "nodestoresize" -> {
                        metrics.add("# HELP neo4j_store_node_store_size_bytes Size of the node store file")
                        metrics.add("# TYPE neo4j_store_node_store_size_bytes gauge")
                        metrics.add("neo4j_store_node_store_size_bytes ${value.asLong()}")
                    }

                    "relationshipstoresize" -> {
                        metrics.add("# HELP neo4j_store_relationship_store_size_bytes Size of the relationship store file")
                        metrics.add("# TYPE neo4j_store_relationship_store_size_bytes gauge")
                        metrics.add("neo4j_store_relationship_store_size_bytes ${value.asLong()}")
                    }

                    "propertystoresize" -> {
                        metrics.add("# HELP neo4j_store_property_store_size_bytes Size of the property store file")
                        metrics.add("# TYPE neo4j_store_property_store_size_bytes gauge")
                        metrics.add("neo4j_store_property_store_size_bytes ${value.asLong()}")
                    }

                    "stringstoresize" -> {
                        metrics.add("# HELP neo4j_store_string_store_size_bytes Size of the string store file")
                        metrics.add("# TYPE neo4j_store_string_store_size_bytes gauge")
                        metrics.add("neo4j_store_string_store_size_bytes ${value.asLong()}")
                    }

                    "arraystoresize" -> {
                        metrics.add("# HELP neo4j_store_array_store_size_bytes Size of the array store file")
                        metrics.add("# TYPE neo4j_store_array_store_size_bytes gauge")
                        metrics.add("neo4j_store_array_store_size_bytes ${value.asLong()}")
                    }

                    "totalstoresize" -> {
                        metrics.add("# HELP neo4j_store_total_size_bytes Total size of all store files")
                        metrics.add("# TYPE neo4j_store_total_size_bytes gauge")
                        metrics.add("neo4j_store_total_size_bytes ${value.asLong()}")
                    }

                    "logsize" -> {
                        metrics.add("# HELP neo4j_store_log_size_bytes Size of transaction log files")
                        metrics.add("# TYPE neo4j_store_log_size_bytes gauge")
                        metrics.add("neo4j_store_log_size_bytes ${value.asLong()}")
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error processing store metrics: ${e.message}")
        }

        return metrics
    }
}