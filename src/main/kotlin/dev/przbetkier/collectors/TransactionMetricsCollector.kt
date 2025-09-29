package dev.przbetkier.collectors

import org.neo4j.driver.Record
import org.neo4j.driver.Session
import org.slf4j.LoggerFactory

class TransactionMetricsCollector : MetricCollector {

    private val logger = LoggerFactory.getLogger(TransactionMetricsCollector::class.java)

    override fun collect(session: Session): List<String> {
        val metrics = mutableListOf<String>()
        try {
            val txResult = session.run("CALL apoc.monitor.tx()")
            if (txResult.hasNext()) {
                val txRecord = txResult.single()
                val txMetrics = convertTxMetricsToPrometheus(txRecord)
                metrics.addAll(txMetrics)
            }
        } catch (e: Exception) {
            logger.error("Error collecting transaction metrics (APOC may not be available): ${e.message}")
        }
        return metrics.toList()
    }

    private fun convertTxMetricsToPrometheus(record: Record): List<String> {
        val metrics = mutableListOf<String>()

        try {
            val keys = record.keys()

            keys.forEach { key ->
                val value = record.get(key)
                when (key) {
                    "rolledBackTx" -> {
                        metrics.add("# HELP neo4j_transactions_rolled_back_total Number of rolled back transactions")
                        metrics.add("# TYPE neo4j_transactions_rolled_back_total counter")
                        metrics.add("neo4j_transactions_rolled_back_total ${value.asLong()}")
                    }

                    "currentOpenedTx" -> {
                        metrics.add("# HELP neo4j_transactions_active Number of currently active transactions")
                        metrics.add("# TYPE neo4j_transactions_active gauge")
                        metrics.add("neo4j_transactions_active ${value.asLong()}")
                    }

                    "peakTx" -> {
                        metrics.add("# HELP neo4j_transactions_peak_active Peak number of concurrent active transactions")
                        metrics.add("# TYPE neo4j_transactions_peak_active gauge")
                        metrics.add("neo4j_transactions_peak_active ${value.asLong()}")
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("Error processing transaction metrics: ${e.message}")
        }
        return metrics
    }
}