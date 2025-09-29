package dev.przbetkier.collectors

import org.neo4j.driver.Session

interface MetricCollector {

    fun collect(session: Session): List<String>
}