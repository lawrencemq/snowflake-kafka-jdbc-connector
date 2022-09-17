package lawrencemq.SnowflakeSinkConnector.sink;

import lawrencemq.SnowflakeSinkConnector.sql.*;

import java.util.*;
import java.util.stream.*;

import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_ACCOUNT;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_DB;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_PASSPHRASE;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_ROLE;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_TABLE;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_USER_NAME;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_WAREHOUSE;

public class TestData {

    final static String DATABASE = "db1";
    final static String SCHEMA = "schema2";
    final static String TABLE_NAME = "table3";

    final static Table TABLE = new Table(DATABASE, SCHEMA, TABLE_NAME);

    static SnowflakeSinkConnectorConfig genConfig() {
        return genConfig(Map.of());
    }
    static SnowflakeSinkConnectorConfig genConfig(Map<?, ?> properties) {
        Map<?, ?> defaultConfigs = Map.of(
                SNOWFLAKE_USER_NAME, "testUser",
                SNOWFLAKE_PASSPHRASE, "butterCup123!",
                SNOWFLAKE_ACCOUNT, "123456789",
                SNOWFLAKE_WAREHOUSE, "testWH",
                SNOWFLAKE_ROLE, "defaultRole",
                SNOWFLAKE_DB, DATABASE,
                SNOWFLAKE_SCHEMA, SCHEMA,
                SNOWFLAKE_TABLE, TABLE_NAME
        );


        Map<?, ?> finalConfigs = Stream.of(defaultConfigs, properties)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        return new SnowflakeSinkConnectorConfig(finalConfigs);
    }
}
