package dev.przbetkier.collectors

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.neo4j.driver.Session
import org.slf4j.LoggerFactory

class JvmMetricsCollector : MetricCollector {

    private val logger = LoggerFactory.getLogger(JvmMetricsCollector::class.java)
    private val objectMapper = ObjectMapper().registerKotlinModule()

    override fun collect(session: Session): List<String> {
        // Collect JVM metrics
        val metrics = mutableListOf<String>()
        val jvmResult =
            session.run("CALL dbms.queryJmx(\"java.lang:*\") YIELD name, attributes RETURN name, attributes")
        while (jvmResult.hasNext()) {
            val record = jvmResult.next()
            val metricName = record.get("name").asString()
            val attributesMap = record.get("attributes").asMap()

            try {
                val attributes = objectMapper.valueToTree<JsonNode>(attributesMap)
                convertJvmMetricsToPrometheus(metricName, attributes).let {
                    metrics.addAll(it)
                }
            } catch (e: Exception) {
                logger.error("Error processing JVM metric $metricName: ${e.message}")
            }
        }
        return metrics.toList()
    }

    private fun convertJvmMetricsToPrometheus(metricName: String, attributes: JsonNode): List<String> {
        val metrics = mutableListOf<String>()
        val cleanMetricName = sanitizeMetricName(metricName)

        attributes.properties().forEach { (key, value) ->
            val metricValue = value.get("value")
            val description = value.get("description")?.asText() ?: "JVM metric"

            when {
                // Threading metrics
                key == "ThreadCount" -> {
                    metrics.add("# HELP ${cleanMetricName}_threads $description")
                    metrics.add("# TYPE ${cleanMetricName}_threads gauge")
                    metrics.add("${cleanMetricName}_threads ${metricValue.asLong()}")
                }

                key == "DaemonThreadCount" -> {
                    metrics.add("# HELP ${cleanMetricName}_daemon_threads $description")
                    metrics.add("# TYPE ${cleanMetricName}_daemon_threads gauge")
                    metrics.add("${cleanMetricName}_daemon_threads ${metricValue.asLong()}")
                }

                key == "TotalStartedThreadCount" -> {
                    metrics.add("# HELP ${cleanMetricName}_threads_started_total $description")
                    metrics.add("# TYPE ${cleanMetricName}_threads_started_total counter")
                    metrics.add("${cleanMetricName}_threads_started_total ${metricValue.asLong()}")
                }

                key == "CurrentThreadAllocatedBytes" -> {
                    metrics.add("# HELP ${cleanMetricName}_thread_allocated_bytes $description")
                    metrics.add("# TYPE ${cleanMetricName}_thread_allocated_bytes gauge")
                    metrics.add("${cleanMetricName}_thread_allocated_bytes ${metricValue.asLong()}")
                }

                // Memory metrics
                key == "HeapMemoryUsage" || key == "NonHeapMemoryUsage" -> {
                    // These are composite values with nested properties
                    if (metricValue.isObject) {
                        val memoryType = if (key == "HeapMemoryUsage") "heap" else "nonheap"
                        val properties = metricValue.get("properties")

                        if (properties != null && properties.isObject) {
                            properties.properties().forEach { (subKey, subValue) ->
                                when (subKey.lowercase()) {
                                    "used" -> {
                                        metrics.add("# HELP ${cleanMetricName}_${memoryType}_used_bytes Memory used in bytes")
                                        metrics.add("# TYPE ${cleanMetricName}_${memoryType}_used_bytes gauge")
                                        metrics.add("${cleanMetricName}_${memoryType}_used_bytes ${subValue.asLong()}")
                                    }

                                    "committed" -> {
                                        metrics.add("# HELP ${cleanMetricName}_${memoryType}_committed_bytes Memory committed in bytes")
                                        metrics.add("# TYPE ${cleanMetricName}_${memoryType}_committed_bytes gauge")
                                        metrics.add("${cleanMetricName}_${memoryType}_committed_bytes ${subValue.asLong()}")
                                    }

                                    "max" -> {
                                        val maxValue = subValue.asLong()
                                        // -1 means no maximum, convert to a very large number or skip
                                        if (maxValue > 0) {
                                            metrics.add("# HELP ${cleanMetricName}_${memoryType}_max_bytes Maximum memory in bytes")
                                            metrics.add("# TYPE ${cleanMetricName}_${memoryType}_max_bytes gauge")
                                            metrics.add("${cleanMetricName}_${memoryType}_max_bytes $maxValue")
                                        }
                                    }

                                    "init" -> {
                                        metrics.add("# HELP ${cleanMetricName}_${memoryType}_init_bytes Initial memory in bytes")
                                        metrics.add("# TYPE ${cleanMetricName}_${memoryType}_init_bytes gauge")
                                        metrics.add("${cleanMetricName}_${memoryType}_init_bytes ${subValue.asLong()}")
                                    }
                                }
                            }
                        }
                    }
                }

                // GC metrics
                key == "CollectionCount" -> {
                    metrics.add("# HELP ${cleanMetricName}_collections_total $description")
                    metrics.add("# TYPE ${cleanMetricName}_collections_total counter")
                    metrics.add("${cleanMetricName}_collections_total ${metricValue.asLong()}")
                }

                key == "CollectionTime" -> {
                    metrics.add("# HELP ${cleanMetricName}_collection_time_ms $description")
                    metrics.add("# TYPE ${cleanMetricName}_collection_time_ms counter")
                    metrics.add("${cleanMetricName}_collection_time_ms ${metricValue.asLong()}")
                }

                // Runtime metrics
                key == "Uptime" -> {
                    metrics.add("# HELP ${cleanMetricName}_uptime_ms $description")
                    metrics.add("# TYPE ${cleanMetricName}_uptime_ms gauge")
                    metrics.add("${cleanMetricName}_uptime_ms ${metricValue.asLong()}")
                }

                key == "StartTime" -> {
                    metrics.add("# HELP ${cleanMetricName}_start_time_ms $description")
                    metrics.add("# TYPE ${cleanMetricName}_start_time_ms gauge")
                    metrics.add("${cleanMetricName}_start_time_ms ${metricValue.asLong()}")
                }

                // Operating System metrics
                key == "ProcessCpuLoad" -> {
                    metrics.add("# HELP ${cleanMetricName}_process_cpu_load $description")
                    metrics.add("# TYPE ${cleanMetricName}_process_cpu_load gauge")
                    metrics.add("${cleanMetricName}_process_cpu_load ${metricValue.asDouble()}")
                }

                key == "SystemCpuLoad" -> {
                    metrics.add("# HELP ${cleanMetricName}_system_cpu_load $description")
                    metrics.add("# TYPE ${cleanMetricName}_system_cpu_load gauge")
                    metrics.add("${cleanMetricName}_system_cpu_load ${metricValue.asDouble()}")
                }

                key == "AvailableProcessors" -> {
                    metrics.add("# HELP ${cleanMetricName}_available_processors $description")
                    metrics.add("# TYPE ${cleanMetricName}_available_processors gauge")
                    metrics.add("${cleanMetricName}_available_processors ${metricValue.asLong()}")
                }

                key == "TotalPhysicalMemorySize" -> {
                    metrics.add("# HELP ${cleanMetricName}_physical_memory_bytes $description")
                    metrics.add("# TYPE ${cleanMetricName}_physical_memory_bytes gauge")
                    metrics.add("${cleanMetricName}_physical_memory_bytes ${metricValue.asLong()}")
                }

                key == "FreePhysicalMemorySize" -> {
                    metrics.add("# HELP ${cleanMetricName}_free_physical_memory_bytes $description")
                    metrics.add("# TYPE ${cleanMetricName}_free_physical_memory_bytes gauge")
                    metrics.add("${cleanMetricName}_free_physical_memory_bytes ${metricValue.asLong()}")
                }

                key == "CommittedVirtualMemorySize" -> {
                    metrics.add("# HELP ${cleanMetricName}_committed_virtual_memory_bytes $description")
                    metrics.add("# TYPE ${cleanMetricName}_committed_virtual_memory_bytes gauge")
                    metrics.add("${cleanMetricName}_committed_virtual_memory_bytes ${metricValue.asLong()}")
                }

                // Handle other numeric values generically
                metricValue.isNumber -> {
                    val suffix = key.lowercase()
                        .replace(Regex("([a-z])([A-Z])"), "$1_$2") // camelCase to snake_case
                        .replace(Regex("[^a-z0-9_]"), "_")
                        .lowercase()

                    val metricType =
                        if (key.contains("Count") || key.contains("Total") || key.contains("Time")) "counter" else "gauge"

                    metrics.add("# HELP ${cleanMetricName}_$suffix $description")
                    metrics.add("# TYPE ${cleanMetricName}_$suffix $metricType")
                    metrics.add("${cleanMetricName}_$suffix ${if (metricValue.isIntegralNumber) metricValue.asLong() else metricValue.asDouble()}")
                }
            }
        }

        return metrics
    }

    private fun sanitizeMetricName(name: String): String {
        // Convert JVM metric names to Prometheus format
        return name
            .replace("java.lang:type=", "jvm_")
            .replace("java.lang:name=", "jvm_")
            .replace(",name=", "_")
            .replace(",type=", "_")
            .replace(".", "_")
            .replace("-", "_")
            .replace(":", "_")
            .replace(" ", "_")
            .lowercase()
    }
}