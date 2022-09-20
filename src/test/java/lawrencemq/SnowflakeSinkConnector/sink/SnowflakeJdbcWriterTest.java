package lawrencemq.SnowflakeSinkConnector.sink;

import lawrencemq.SnowflakeSinkConnector.sql.*;
import org.apache.kafka.connect.sink.*;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static lawrencemq.SnowflakeSinkConnector.TestData.TABLE;
import static lawrencemq.SnowflakeSinkConnector.TestData.genConfig;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SnowflakeJdbcWriterTest {

    PreparedStatement preparedStatement;
    Connection connection;
    ConnectionManager connectionManager;
    TableManager tableManager;


    @BeforeEach
    void prepareMocks() throws SQLException {
        preparedStatement = mock(PreparedStatement.class);
        connection = mock(Connection.class);
        connectionManager = mock(ConnectionManager.class);
        tableManager = mock(TableManager.class);


        when(preparedStatement.executeBatch()).thenReturn(new int[]{Statement.SUCCESS_NO_INFO});
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(connectionManager.getConnection()).thenReturn(connection);
        when(tableManager.getTable()).thenReturn(TABLE);

    }

    @Test
    void write() throws SQLException {
        SnowflakeJdbcWriter writer = new SnowflakeJdbcWriter(
                genConfig(),
                tableManager,
                connectionManager
        );

        List<SinkRecord> sinkRecords = RecordBufferTest.generateMessages(10);

        writer.write(sinkRecords);

        verify(tableManager).createOrAmendTable(eq(connection), any(KafkaFieldsMetadata.class));
        verify(preparedStatement).executeBatch();
        verify(preparedStatement).close();
        verify(connection).commit();
    }

    @Test
    void writeDoesRollbackOnError() throws SQLException {
        when(preparedStatement.executeBatch()).thenThrow(SQLException.class);
        doThrow(SQLException.class).when(connection).rollback();

        SnowflakeJdbcWriter writer = new SnowflakeJdbcWriter(
                genConfig(),
                tableManager,
                connectionManager
        );

        List<SinkRecord> sinkRecords = RecordBufferTest.generateMessages(10);

        assertThrows(SQLException.class,
                () -> writer.write(sinkRecords),
                "Should catch any SQL exception and throw it upwards"
        );

        verify(connection).rollback();
        verify(connection, never()).commit();
    }

    @Test
    void close() {

        SnowflakeJdbcWriter writer = new SnowflakeJdbcWriter(
                genConfig(),
                tableManager,
                connectionManager
        );

        writer.close();

        verify(connectionManager).close();
    }
}