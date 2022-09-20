package lawrencemq.SnowflakeSinkConnector.sql;

import lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig;
import org.apache.kafka.connect.errors.*;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_ACCOUNT;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_DB;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_RETRY_BACKOFF_MS;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_ROLE;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_TABLE;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_USER_NAME;
import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.SNOWFLAKE_WAREHOUSE;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectionManagerTest {

    private final static String DATABASE = "test_db";
    private final static String SCHEMA = "test_schema";
    private final static String TABLE_NAME = "test_table";


    private static SnowflakeSinkConnectorConfig genConfig(Map<?, ?> properties) {
        Map<?, ?> defaultConfigs = Map.of(
                SNOWFLAKE_USER_NAME, "testUser",
                SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE, "butterCup123!",
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

    @Test
    void getConnection() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isValid(anyInt())).thenReturn(true);
        ConnectionManager manager = new ConnectionManager(genConfig(Map.of())){
            @Override
            protected Connection getSnowflakeConnection(String url, Properties props) {
                return connection;
            }
        };

        // Ensures connects successfully
        assertSame(manager.getConnection(), connection);

        // Ensures stores the connection for a fast retrieval for other threads
        assertSame(manager.getConnection(), connection);
    }

    @Test
    void getConnectionRetries() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isValid(anyInt())).thenReturn(true);
        ConnectionManager manager = spy(new ConnectionManager(genConfig(Map.of(SNOWFLAKE_RETRY_BACKOFF_MS, 0))){
            @Override
            protected Connection getSnowflakeConnection(String url, Properties props) throws SQLException {
                throw new SQLException("test error");
            }
        });

        assertThrows(ConnectException.class,
                () -> manager.getConnection(),
                "After failing retrying connection several times, should throw an error");


        // Verifying tried reconnecting
        verify(manager, times(3)).getSnowflakeConnection(anyString(), any());
    }

    @Test
    void getConnectionReconnectsWhenConnectionInvalid() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isValid(anyInt())).thenThrow(SQLException.class);
        ConnectionManager manager = spy(new ConnectionManager(genConfig(Map.of(SNOWFLAKE_RETRY_BACKOFF_MS, 0))){
            @Override
            protected Connection getSnowflakeConnection(String url, Properties props) throws SQLException {
                return connection;
            }
        });

        manager.getConnection(); // setting up initial connection;
        manager.getConnection(); // getting connection that's been invalidated to force reconnect

        // Verifying connected twice
        verify(manager, times(2)).getSnowflakeConnection(anyString(), any());
    }

    @Test
    void close() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isValid(anyInt())).thenReturn(true);
        ConnectionManager manager = new ConnectionManager(genConfig(Map.of())){
            @Override
            protected Connection getSnowflakeConnection(String url, Properties props){
                return connection;
            }
        };

        // Will not close when no connection
        manager.close();
        verify(connection, times(0)).close();

        // Will close open connection
        manager.getConnection();
        manager.close();
        verify(connection, times(1)).close();
    }

    @Test
    void closeExceptionPassedQuietly() throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.isValid(anyInt())).thenReturn(true);
        doThrow(SQLException.class).when(connection).close();
        ConnectionManager manager = new ConnectionManager(genConfig(Map.of())){
            @Override
            protected Connection getSnowflakeConnection(String url, Properties props){
                return connection;
            }
        };

        // Will close open connection
        manager.getConnection();
        manager.close();
        verify(connection).close();

    }
}