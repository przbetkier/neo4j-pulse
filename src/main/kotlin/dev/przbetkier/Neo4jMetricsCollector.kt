package dev.przbetkier

import dev.przbetkier.collectors.JvmMetricsCollector
import dev.przbetkier.collectors.MetaStatsMetricsCollector
import dev.przbetkier.collectors.QueriesMetricsCollector
import dev.przbetkier.collectors.StoreMetricsCollector
import dev.przbetkier.collectors.TransactionMetricsCollector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import java.text.SimpleDateFormat
import java.util.Date

class Neo4jMetricsCollector(
    private val config: Neo4jConfig
) {

    private val driver: Driver = GraphDatabase.driver(
        config.uri,
        AuthTokens.basic(config.username, config.password)
    )

    private val jvmMetricsCollector = JvmMetricsCollector()
    private val storeMetricsCollector = StoreMetricsCollector()
    private val transactionMetricsCollector = TransactionMetricsCollector()
    private val queriesMetricsCollector = QueriesMetricsCollector()
    private val metaStatsMetricsCollector = MetaStatsMetricsCollector()

    fun collectMetrics(): String {
        val metrics = mutableListOf<String>()

        driver.session(SessionConfig.forDatabase(config.database)).use { session ->
            listOf(
                { jvmMetricsCollector.collect(session) },
                { storeMetricsCollector.collect(session) },
                { transactionMetricsCollector.collect(session) },
                { queriesMetricsCollector.collect(session) },
                { metaStatsMetricsCollector.collect(session) }
            ).forEach { collector ->
                metrics.addAll(collector())
            }
        }

        return buildPrometheusOutput(metrics)
    }

    private fun buildPrometheusOutput(metrics: List<String>): String {
        val timestamp = SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Date())
        val header = listOf(
            "# JVM & Neo4j Metrics Export",
            "# Generated at: $timestamp",
            "# Source: JVM JMX + Neo4j APOC/DBMS via Cypher queries",
            ""
        )
        return (header + metrics).joinToString("\n") + "\n"
    }

    fun close() {
        driver.close()
    }
}