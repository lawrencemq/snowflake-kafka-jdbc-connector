# Kafka to Snowflake JDBC Connector

snowflake-kafka-jdbc is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data directly into Snowflake, matching schemas 1-to-1 in Kafka. 
Each instance of a connector will handle a single dataset and write to a corresponding table. 


# About

This connector takes Avro data from a Kafka topic and writes it directly to Snowflake using the JDBC driver Snowflake provides. 

Each instance is responsible for a single table. Kafka Connect may run multiple connectors, and thus multiple tables and datasets. 

This is an alternative to the [snowflake-kafka-connector](https://github.com/snowflakedb/snowflake-kafka-connector) which writes all data to a [Snowflake stage](https://docs.snowflake.com/en/user-guide/kafka-connector.html), utilizes SnowPipe to process and move the data to the user's desired location in their Snowflake instance. All data is written using an [aggregated and simplified schema](https://docs.snowflake.com/en/user-guide/kafka-connector-overview.html#schema-of-tables-for-kafka-topics). But with this comes difficulty to write queries or the need to flatten data. This can be time consuming and expensive. 

In this approach, Connect will write directly to the user's Snowflake instance using the [JDBC](https://docs.snowflake.com/en/user-guide/jdbc.html) driver, allowing for data to be written once and in a schema that is 1-to-1 to that in Kafka.

Additionally, as schemas evolve (with compatibility) in Kafka, their schemas in Snowflake will be automatically updated.

Kafka metadata (topic, partition, offset, create-time) are also passed to Snowflake by default. This is generally considered best practice and can be useful in deduplication, but this may be turned off at any time to save space.

Find more documentation for this connector on the Confluent wiki [here](#)



# Connector Parameters

## Required
- Username:  the username
- Account: The Snowflake account identifier, e.g,. (ARDDZWW-LA06329)
- Warehouse: The Snowflake warehouse to use to execute SQL queries
- Role: The Snowflake role the user will assume once connected
- Database: Database location to which data will be written
- Schema: Schema location to which data will be written
- Table: Table location to which data will be written

## Security

- Passphrase: Password for the user account. Alternatively, one can authenticate using a private key (see below).
- Private Key File: FQDN of the private key associated with the user account. Alternatively, one can authenticate using a passphrase (see above).
- Private Key File Passphrase: If using a private key that is encrypted, add the decryption passphrase here. Note: not all private-key files are encrypted. 

## Optional

- SNOWFLAKE_MAX_RETRIES: Number of times connector will retry an operation against Snowflake upon failure. Default: 3 
- SNOWFLAKE_RETRY_BACKOFF_MS: Additional delay (in milliseconds) connector will wait before reconnecting to Snowflake upon failure. Default: 5,000  
- BATCH_SIZE: Maximum number of rows to be added in a single insert statement. Default: 3,000 
- AUTO_CREATE: Allows table to be created by the connector if nonexistent in Snowflake. Default: true 
- AUTO_EVOLVE: Allows table schema to automatically evolve as Kafka schema evolves. Default: true 
- IGNORE_KEY: Connector will ignore any data in the Kafka key if true - helpful for non-Avro keys. Default: false 
- IGNORE_KAFKA_METADATA: Connector will skip adding Kafka metadata (topic, partition, offset, create-time) if true. Default: false 

# Local Development

The Confluent platform is required to test locally as well as a Snowflake account with ample user permissions.

At the time of this writing, Confluent 7.2.2 is used. 

Follow the [quickstart](https://docs.confluent.io/platform/current/platform-quickstart.html#prerequisites) instructions for Confluent to set up a cluster locally - use the `tar archive` over the Docker. 

To build a development snapshot, use Maven a JAR. Copy that JAR into the Confluence binaries and restart Connect to make it available through the UI. 

- `mvn clean compile assembly:single`
- `cp ./target/snowflake-kafka-jdbc-connector-0.2.0-SNAPSHOT.jar $CONFLUENT_HOME/share/java/snowflake-kafka-jdbc-connector`
- `confluent local services connect stop && confluent local services connect start`

# Known Limitations

## Batching
Schemas that use complex types (array, maps, structs, or any non-primitive) may see poor insert performance in Snowflake. The JDBC driver [does not have functionality](https://community.snowflake.com/s/question/0D50Z00008hAW7mSAG/are-there-plans-to-implement-array-functionality-for-the-jdbc-driver) to add these data types directly (yet). 
As a workaround, complex types can first be selected and parsed using SQL Select statements and then inserted. Users will find that multiple queries are executed consecutively on each batch insert for these data types, and they may need to adjust the Kafka Consumer configs of the connector to allow for more time for the insertions to complete.

Once this functionality is implemented in the JDBC connector, this connector will be updated to utilize the functionality and become even more efficient.

In the meantime, if an Avro schema uses only primitives, batch insertions are efficient will N rows in a single insert statement, where N is the Batch Size in the configuration.


# Contribute

At this time, all PRs are at the discretion of the owner. But please, we hope this project grows and are looking for others to help own and maintain. If you feel you are able to help, please reach out to the owner. 

- Source Code: https://github.com/lawrencemq/snowflake-kafka-jdbc-connector
- Issue Tracker: https://github.com/lawrencemq/snowflake-kafka-jdbc-connector/issues


# License

This project is licensed under the [Apache License 2.0](LICENSE).
