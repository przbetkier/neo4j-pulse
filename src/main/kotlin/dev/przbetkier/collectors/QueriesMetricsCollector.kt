package dev.przbetkier.collectors

import org.neo4j.driver.Session
import org.slf4j.LoggerFactory

class QueriesMetricsCollector : MetricCollector {

    private val logger = LoggerFactory.getLogger(QueriesMetricsCollector::class.java)

    override fun collect(session: Session): List<String> {
        val metrics = mutableListOf<String>()
        try {
            val queriesResult = session.run("CALL dbms.listQueries()")
            val activeQueriesCount = queriesResult.list().size
            metrics.add("# HELP neo4j_active_queries_total Number of currently active queries")
            metrics.add("# TYPE neo4j_active_queries_total gauge")
            metrics.add("neo4j_active_queries_total $activeQueriesCount")
        } catch (e: Exception) {
            logger.error("Error collecting active queries metrics: ${e.message}")
        }
        return metrics.toList()
    }
}