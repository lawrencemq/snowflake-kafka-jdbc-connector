package lawrencemq.SnowflakeJdbcSinkConnector;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.*;
import lawrencemq.SnowflakeJdbcSinkConnector.sql.*;

import java.util.*;
import java.util.stream.*;

import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_ACCOUNT;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_DB;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_ROLE;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_SCHEMA;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_TABLE;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_USER_NAME;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig.SNOWFLAKE_WAREHOUSE;

public class TestData {

    final public static String DATABASE = "db1";
    final public static String SCHEMA = "schema2";
    final public static String TABLE_NAME = "table3";

    final public static Table TABLE = new Table(DATABASE, SCHEMA, TABLE_NAME);
    final public static Map<String, String> DEFAULT_CONFIGS = Map.of(
            SNOWFLAKE_USER_NAME, "testUser",
            SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE, "butterCup123!",
            SNOWFLAKE_ACCOUNT, "123456789",
            SNOWFLAKE_WAREHOUSE, "testWH",
            SNOWFLAKE_ROLE, "defaultRole",
            SNOWFLAKE_DB, DATABASE,
            SNOWFLAKE_SCHEMA, SCHEMA,
            SNOWFLAKE_TABLE, TABLE_NAME
    );

    public static SnowflakeJdbcSinkConnectorConfig genConfig() {
        return genConfig(Map.of());
    }

    public static SnowflakeJdbcSinkConnectorConfig genConfig(Map<?, ?> properties) {
        Map<?, ?> finalConfigs = Stream.of(DEFAULT_CONFIGS, properties)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        return new SnowflakeJdbcSinkConnectorConfig(finalConfigs);
    }
}
