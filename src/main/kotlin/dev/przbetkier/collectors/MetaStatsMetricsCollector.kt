package dev.przbetkier.collectors

import org.neo4j.driver.Record
import org.neo4j.driver.Session
import org.slf4j.LoggerFactory

class MetaStatsMetricsCollector : MetricCollector {

    private val logger = LoggerFactory.getLogger(MetaStatsMetricsCollector::class.java)

    override fun collect(session: Session): List<String> {
        val metrics = mutableListOf<String>()
        try {
            val metaResult = session.run("CALL apoc.meta.stats()")
            if (metaResult.hasNext()) {
                val metaRecord = metaResult.single()
                val metaMetrics = convertMetaStatsToPrometheus(metaRecord)
                metrics.addAll(metaMetrics)
            }
        } catch (e: Exception) {
            logger.error("Error collecting meta stats (APOC may not be available): ${e.message}")
        }
        return metrics.toList()
    }

    private fun convertMetaStatsToPrometheus(record: Record): List<String> {
        val metrics = mutableListOf<String>()

        try {
            val keys = record.keys()

            keys.forEach { key ->
                val value = record.get(key)
                when (key.lowercase()) {
                    "nodecount" -> {
                        metrics.add("# HELP neo4j_nodes_total Total number of nodes in the database")
                        metrics.add("# TYPE neo4j_nodes_total gauge")
                        metrics.add("neo4j_nodes_total ${value.asLong()}")
                    }

                    "relcount" -> {
                        metrics.add("# HELP neo4j_relationships_total Total number of relationships in the database")
                        metrics.add("# TYPE neo4j_relationships_total gauge")
                        metrics.add("neo4j_relationships_total ${value.asLong()}")
                    }

                    "labelcount" -> {
                        metrics.add("# HELP neo4j_labels_total Total number of distinct labels")
                        metrics.add("# TYPE neo4j_labels_total gauge")
                        metrics.add("neo4j_labels_total ${value.asLong()}")
                    }

                    "reltypecount" -> {
                        metrics.add("# HELP neo4j_relationship_types_total Total number of distinct relationship types")
                        metrics.add("# TYPE neo4j_relationship_types_total gauge")
                        metrics.add("neo4j_relationship_types_total ${value.asLong()}")
                    }

                    "propertykeynamecount" -> {
                        metrics.add("# HELP neo4j_property_keys_total Total number of distinct property keys")
                        metrics.add("# TYPE neo4j_property_keys_total gauge")
                        metrics.add("neo4j_property_keys_total ${value.asLong()}")
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error processing meta stats: ${e.message}", e)
        }

        return metrics
    }
}