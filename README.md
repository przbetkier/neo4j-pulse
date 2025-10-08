# Neo4j Pulse - Neo4j Metrics Exporter

A lightweight Kotlin application that bridges the monitoring gap between Neo4j Community Edition and Prometheus by exposing JVM and Neo4j database metrics in Prometheus-compatible format.

Neo4j Pulse has no affiliation with either Neo4j USA or Neo4j Sweden.

## Overview

Neo4j Community Edition lacks built-in metrics export capabilities (available only in Enterprise Edition via CSV files and Prometheus endpoints). This project solves that limitation by leveraging Cypher queries to extract metrics directly from Neo4j's JMX interface and APOC procedures, then exposing them through a standard Prometheus scrape endpoint.

## Why This Project?

- **Community Edition Support**: Get comprehensive metrics without requiring Enterprise Edition
- **Prometheus Integration**: Native support for modern monitoring stacks
- **Zero Neo4j Configuration**: No need to modify Neo4j settings or install additional plugins beyond APOC
- **Comprehensive Coverage**: Monitors both JVM performance and Neo4j database internals

## What It Monitors

### JVM Metrics (via `dbms.queryJmx`)
- **Memory**: Heap and non-heap usage, committed, max values
- **Garbage Collection**: Collection counts and time for all GC types
- **Threading**: Active threads, daemon threads, thread allocation
- **CPU**: Process and system CPU load
- **Operating System**: Physical memory, available processors

### Neo4j Database Metrics (via APOC & DBMS procedures)
- **Store Statistics**: Node store, relationship store, property store sizes
- **Transaction Activity**: Active, committed (read/write), and rolled-back transactions
- **Query Activity**: Count of currently running queries
- **Database Size**: Total node count, relationship count, labels, relationship types

## How It Works

The exporter connects to your Neo4j database and executes several Cypher queries:

1. **`CALL dbms.queryJmx("java.lang:*")`** - Retrieves JVM metrics from the JMX interface
2. **`CALL apoc.monitor.store()`** - Gets store file sizes
3. **`CALL apoc.monitor.tx()`** - Collects transaction statistics
4. **`CALL dbms.listQueries()`** - Counts active queries
5. **`CALL apoc.meta.stats()`** - Retrieves database cardinality statistics

These metrics are parsed, transformed into Prometheus format, and exposed on port 4242 at the `/` endpoint for Prometheus to scrape.


## Quick Start

### Using Docker (Recommended)
TBD

### Using Docker Compose (Full Stack) 
TBD

### Building from Source

```bash
./gradlew build
./gradlew run
```

## Configuration

### Environment Variables
- `NEO4J_URI` - Neo4j connection URI (default: `bolt://localhost:7687`)
- `NEO4J_USERNAME` - Database username (default: `neo4j`)
- `NEO4J_PASSWORD` - Database password (default: `password`)
- `NEO4J_DATABASE` - Target database (default: `neo4j`)

### Configuration File
Alternatively, create a `config.yml` file:

```yaml
neo4j:
  uri: bolt://localhost:7687
  username: neo4j
  password: your-password
  database: neo4j
```

## Prerequisites

- **Neo4j Community Edition 4.x or 5.x** with APOC plugin installed for wider metric coverage
- **Java 23 or higher** (for building/running)
- **Docker** (optional, for containerized deployment)

## Endpoints

- `http://localhost:4242/` - Prometheus metrics endpoint
- `http://localhost:4242/health` - Health check endpoint
- `http://localhost:4242/info` - Service information

## Example Metrics

```prometheus
# JVM Memory
jvm_memory_heap_used_bytes 819429440
jvm_memory_heap_committed_bytes 8589934592
jvm_memory_heap_max_bytes 8589934592

# Neo4j Database
neo4j_nodes_total 1500000
neo4j_relationships_total 3200000
neo4j_transactions_active 5
neo4j_active_queries_total 2

# Store Sizes
neo4j_store_total_size_bytes 2147483648
neo4j_store_node_store_size_bytes 524288000

# Transaction Rates
neo4j_transactions_committed_write_total 125000
neo4j_transactions_committed_read_total 480000
```

## Use Cases

- **Performance Monitoring**: Track memory usage, GC pressure, and query activity
- **Capacity Planning**: Monitor database growth and resource utilization
- **Troubleshooting**: Identify lock contention, long-running queries, and transaction bottlenecks
- **Alerting**: Set up Prometheus alerts for critical metrics
- **SLA Compliance**: Track uptime and performance metrics


## License
Apache 2.0 License. See `LICENSE` file for details.

## Contributing
Contributions are welcome! Please feel free to submit issues or pull requests.

---

**Note**: This exporter requires the APOC plugin to be installed and enabled in your Neo4j instance for full functionality. JVM metrics are available without APOC, but Neo4j-specific metrics (store, transactions) require APOC procedures.