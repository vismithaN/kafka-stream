# Kafka Stream - Driver Location Event Processing

A real-time data streaming application that processes driver location events using Apache Kafka. The system ingests JSON-formatted driver location data from trace files and routes events to different Kafka topics based on event types.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup and Installation](#setup-and-installation)
- [Usage](#usage)
- [Data Format](#data-format)
- [Configuration](#configuration)
- [Testing](#testing)
- [License](#license)

## Overview

This project demonstrates a streaming data pipeline using Apache Kafka to process driver location events. It reads JSON-formatted events from trace files and distributes them to appropriate Kafka topics based on the event type, enabling real-time processing and analysis of driver location data.

The system handles three types of events:
- **DRIVER_LOCATION**: Real-time location updates from drivers
- **ENTERING_BLOCK**: Events when a driver enters a geographical block
- **LEAVING_BLOCK**: Events when a driver leaves a geographical block

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Data Flow Architecture                   │
└─────────────────────────────────────────────────────────────────┘

    ┌──────────────┐
    │  Trace File  │
    │  (JSON Data) │
    └──────┬───────┘
           │
           │ Read events line by line
           │
           ▼
    ┌──────────────────────┐
    │   DataProducer       │
    │   (Java Application) │
    │                      │
    │  ┌────────────────┐  │
    │  │ Parse JSON     │  │
    │  │ Extract "type" │  │
    │  │ Route to topic │  │
    │  └────────────────┘  │
    └──────┬───────────────┘
           │
           │ Send to Kafka
           │
           ▼
    ┌──────────────────────┐
    │   Apache Kafka       │
    │   Message Broker     │
    └──────┬───────────────┘
           │
           ├─────────────────┬─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │   events    │   │driver-      │   │  ZooKeeper  │
    │   topic     │   │locations    │   │  (metadata) │
    │             │   │  topic      │   │             │
    │ ENTERING_   │   │             │   └─────────────┘
    │   BLOCK     │   │ DRIVER_     │
    │             │   │ LOCATION    │
    │ LEAVING_    │   │             │
    │   BLOCK     │   │             │
    └─────┬───────┘   └──────┬──────┘
          │                  │
          └──────┬───────────┘
                 │
                 ▼
         ┌───────────────┐
         │  Consumers    │
         │  (Stream      │
         │  Processing)  │
         └───────────────┘

Event Routing Logic:
─────────────────────
• type == "ENTERING_BLOCK" → events topic (partition based on blockId)
• type == "LEAVING_BLOCK"  → events topic (partition based on blockId)
• type == "DRIVER_LOCATION" → driver-locations topic (partition based on driverId)
```

## Features

- **Real-time Event Processing**: Streams driver location events in real-time using Apache Kafka
- **Topic-based Routing**: Automatically routes events to appropriate topics based on event type
- **Partition Strategy**: Uses smart partitioning based on blockId or driverId for efficient data distribution
- **JSON Support**: Handles JSON-formatted event data with flexible schema
- **Scalable Architecture**: Built on Apache Kafka for horizontal scalability
- **KRaft Mode Support**: Supports modern Kafka deployment without ZooKeeper (KRaft mode)
- **Unit Testing**: Comprehensive test coverage using JUnit and Kafka MockProducer

## Prerequisites

- **Java Development Kit (JDK)**: Version 1.8 or higher
- **Apache Maven**: Version 3.x for building the project
- **Apache Kafka**: Version 0.10.1.0 or higher (included in the `kafka/` directory)
- **Operating System**: Linux, macOS, or Windows with WSL

## Project Structure

```
kafka-stream/
├── kafka/                          # Apache Kafka distribution
│   ├── bin/                        # Kafka executables and scripts
│   ├── config/                     # Kafka configuration files
│   │   ├── server.properties       # Kafka broker configuration
│   │   ├── zookeeper.properties    # ZooKeeper configuration
│   │   ├── producer.properties     # Producer configuration
│   │   ├── consumer.properties     # Consumer configuration
│   │   └── kraft/                  # KRaft mode configurations
│   │       └── README.md           # KRaft mode documentation
│   ├── libs/                       # Kafka library dependencies
│   └── licenses/                   # License files
│
└── stream-processing/
    └── DataProducer/               # Data producer application
        ├── pom.xml                 # Maven project configuration
        ├── tracefile               # Production trace data
        ├── test_trace              # Test trace data
        ├── references              # Citation and reference template
        └── src/
            ├── main/java/
            │   ├── DataProducer.java        # Core producer logic
            │   └── DataProducerRunner.java  # Application entry point
            └── test/java/
                └── DataProducerTest.java    # Unit tests
```

## Setup and Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd kafka-stream
```

### 2. Start Kafka (Traditional ZooKeeper Mode)

#### Start ZooKeeper

```bash
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

#### Start Kafka Broker (in a new terminal)

```bash
cd kafka
bin/kafka-server-start.sh config/server.properties
```

### 3. Start Kafka (KRaft Mode - Alternative)

For a simplified setup without ZooKeeper, see the [KRaft mode documentation](kafka/config/kraft/README.md).

```bash
cd kafka

# Generate a cluster ID
KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)

# Format storage directories
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Start Kafka server
bin/kafka-server-start.sh config/kraft/server.properties
```

### 4. Create Kafka Topics

```bash
cd kafka

# Create the events topic
bin/kafka-topics.sh --create \
  --topic events \
  --bootstrap-server localhost:9092 \
  --partitions 5 \
  --replication-factor 1

# Create the driver-locations topic
bin/kafka-topics.sh --create \
  --topic driver-locations \
  --bootstrap-server localhost:9092 \
  --partitions 5 \
  --replication-factor 1

# Verify topics were created
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 5. Build the Data Producer

```bash
cd stream-processing/DataProducer
mvn clean package
```

## Usage

### Running the Data Producer

After building the project and starting Kafka, run the data producer to start streaming events:

```bash
cd stream-processing/DataProducer
mvn exec:java -Dexec.mainClass="DataProducerRunner"
```

The producer will:
1. Read events from the `tracefile`
2. Parse each JSON event
3. Route events to the appropriate Kafka topic
4. Continue until all events are processed

### Consuming Messages

To verify that messages are being produced correctly, you can use Kafka's console consumer:

```bash
cd kafka

# Consume from the events topic
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic events \
  --from-beginning

# Consume from the driver-locations topic (in a new terminal)
bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic driver-locations \
  --from-beginning
```

## Data Format

### Input Format (Trace File)

Each line in the trace file is a JSON object representing a driver event:

```json
{"driverId":9351,"blockId":414,"latitude":40.7077607,"longitude":-74.0042783,"type":"DRIVER_LOCATION"}
{"blockId":1022,"driverId":9089,"latitude":40.7168212,"longitude":-73.9977594,"type":"LEAVING_BLOCK","status":"AVAILABLE"}
{"blockId":1021,"driverId":9089,"latitude":40.7166518,"longitude":-73.9978653,"type":"ENTERING_BLOCK","status":"AVAILABLE","rating":4.39,"salary":65,"gender":"M"}
```

**Note**: Field order may vary in the actual trace files, as JSON parsing is order-independent.

### Event Types

#### DRIVER_LOCATION
- **Purpose**: Track real-time driver locations
- **Required Fields**: `driverId`, `blockId`, `latitude`, `longitude`, `type`
- **Destination**: `driver-locations` topic
- **Partition Key**: `driverId`

#### ENTERING_BLOCK
- **Purpose**: Record when a driver enters a geographical block
- **Required Fields**: `blockId`, `driverId`, `latitude`, `longitude`, `type`
- **Optional Fields**: `status`, `rating`, `salary`, `gender`
- **Destination**: `events` topic
- **Partition Key**: `blockId`

#### LEAVING_BLOCK
- **Purpose**: Record when a driver leaves a geographical block
- **Required Fields**: `blockId`, `driverId`, `latitude`, `longitude`, `type`
- **Optional Fields**: `status`
- **Destination**: `events` topic
- **Partition Key**: `blockId`

## Configuration

### Kafka Broker Configuration

Edit `kafka/config/server.properties` to customize:

```properties
# Broker ID
broker.id=0

# Listener
listeners=PLAINTEXT://localhost:9092

# Log directory
log.dirs=/tmp/kafka-logs

# Number of partitions per topic
num.partitions=5

# Replication factor
default.replication.factor=1
```

### Producer Configuration

The DataProducer application requires a Kafka producer to be instantiated in the `DataProducerRunner.java` file. Example producer configuration:

```java
// In DataProducerRunner.java - create and configure the producer
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("acks", "all");  // Wait for all replicas to acknowledge

Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
DataProducer dataProducer = new DataProducer(kafkaProducer, "tracefile");
```

Customize these settings in the `DataProducerRunner.java` file as needed.

## Testing

### Run Unit Tests

```bash
cd stream-processing/DataProducer
mvn test
```

The test suite includes:
- **DataProducerTest**: Verifies correct routing of events to topics
- Uses Kafka's MockProducer for isolated testing
- Validates partition assignment based on event type

### Manual Testing

1. Use the `test_trace` file for testing:
```bash
# Modify DataProducerRunner.java to use test_trace instead of tracefile
DataProducer dataProducer = new DataProducer(kafkaProducer, "test_trace");
```

2. Run the producer and verify output in Kafka topics
3. Check that events are correctly partitioned

## Advanced Topics

### Monitoring and Management

Use Kafka's built-in tools for monitoring:

```bash
# View consumer groups
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# View topic details
bin/kafka-topics.sh --describe --topic events --bootstrap-server localhost:9092

# Check broker status
bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

### Performance Tuning

For production deployments, consider:
- Increasing the number of partitions for higher throughput
- Adjusting batch size and linger time in producer configuration
- Configuring appropriate replication factors for fault tolerance
- Tuning JVM heap size for Kafka brokers

### Stream Processing

This producer can be integrated with:
- **Kafka Streams API**: For stateful stream processing
- **Apache Flink**: For complex event processing
- **Apache Spark Streaming**: For batch and stream processing
- **KSQL**: For SQL-based stream processing

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure Kafka broker is running
   - Verify `bootstrap.servers` configuration
   - Check firewall settings

2. **Topic Not Found**
   - Create topics manually using `kafka-topics.sh`
   - Enable auto topic creation in `server.properties`

3. **Out of Memory Errors**
   - Increase JVM heap size in Kafka startup scripts
   - Reduce batch size in producer configuration

## License

This project includes Apache Kafka, which is licensed under the Apache License 2.0. See the `kafka/LICENSE` file for details.

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka Producer API](https://kafka.apache.org/documentation/#producerapi)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [KRaft Mode (KIP-500)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum)

## Contributing

When contributing to this project, please:
1. Follow the existing code style
2. Add unit tests for new features
3. Update documentation as needed
4. Cite references appropriately (see `references` file)

---

**Note**: This project is designed for educational purposes and demonstrates Kafka streaming concepts. For production use, additional considerations for security, monitoring, and fault tolerance should be implemented.
