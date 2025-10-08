package dev.przbetkier

import io.ktor.http.ContentType.Text
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml
import java.io.File

private val logger = LoggerFactory.getLogger(Neo4jConfig::class.java)

data class Neo4jConfig(
    val uri: String,
    val username: String,
    val password: String,
    val database: String = "neo4j"
)

fun loadConfig(): Neo4jConfig {
    // First try to load from environment variables
    val envUri = System.getenv("NEO4J_URI")
    val envUsername = System.getenv("NEO4J_USERNAME")
    val envPassword = System.getenv("NEO4J_PASSWORD")
    val envDatabase = System.getenv("NEO4J_DATABASE")

    if (!envUri.isNullOrBlank() && !envUsername.isNullOrBlank() && !envPassword.isNullOrBlank()) {
        logger.info("Using configuration from environment variables")
        return Neo4jConfig(
            uri = envUri,
            username = envUsername,
            password = envPassword,
            database = envDatabase ?: "neo4j"
        )
    }

    val configFile = File("config.yml")
    if (!configFile.exists()) {
        // Create default config file
        val defaultConfig = """
            neo4j:
              uri: bolt://localhost:7687
              username: neo4j
              password: password
              database: neo4j
        """.trimIndent()

        configFile.writeText(defaultConfig)
        logger.warn("Created default config.yml file. Please update it with your Neo4j connection details.")
    }

    val yaml = Yaml()
    val config = yaml.load<Map<String, Map<String, String>>>(configFile.readText())
    val neo4jConfig = config["neo4j"]

    return Neo4jConfig(
        uri = neo4jConfig?.get("uri") ?: "bolt://localhost:7687",
        username = neo4jConfig?.get("username") ?: "neo4j",
        password = neo4jConfig?.get("password") ?: "password",
        database = neo4jConfig?.get("database") ?: "neo4j"
    )
}

open class Neo4jPulseRunner {

    companion object {
        private val logger = LoggerFactory.getLogger(Neo4jPulseRunner::class.java)

        @JvmStatic
        fun main(args: Array<String>) {
            logger.info("\u001B[34m Neo4j Pulse - Metric Exporter for Neo4j \u001B[0m")

            val config = loadConfig()
            val collector = Neo4jMetricsCollector(config)

            val server = embeddedServer(Netty, port = 4242) {
                routing {
                    get("/") {
                        try {
                            val metrics = collector.collectMetrics()
                            call.respondText(metrics, Text.Plain)
                        } catch (e: Exception) {
                            call.respondText(
                                "Error collecting metrics: ${e.message}",
                                Text.Plain,
                                InternalServerError
                            )
                        }
                    }

                    get("/info") {
                        call.respondText(
                            """
                    Neo4j Community Edition Metrics Exporter
                    
                    Available endpoints:
                    - GET / - Prometheus format metrics
                    - GET /health - Health check
                    
                    Configuration: config.yml
                    Port: 4242
                """.trimIndent()
                        )
                    }

                    get("/health") {
                        call.respondText("OK")
                    }
                }
            }

            Runtime.getRuntime().addShutdownHook(Thread {
                logger.info("Shutting down...")
                collector.close()
                server.stop(1000, 2000)
            })

            logger.info("Server starting on port 4242")
            logger.info("Metrics endpoint: http://localhost:4242/")
            logger.info("Metrics endpoint: http://localhost:4242/info")
            logger.info("Health check: http://localhost:4242/health")

            server.start(wait = true)
        }
    }
}
