package lawrencemq.SnowflakeSinkConnector.sink;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.*;
import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.BATCH_SIZE;
import static lawrencemq.SnowflakeSinkConnector.sink.TestData.genConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecordBufferTest {


    private final static String TOPIC = "topic1";
    private final static int PARTITION = 21;

    private final static Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("keySchema")
            .field("id", SchemaBuilder.string().name("id").build())
            .build();

    private final static Schema EARLY_VALUE_SCHEMA = SchemaBuilder.struct()
            .name("keySchema")
            .field("firstName", SchemaBuilder.string().name("firstName").build())
            .field("lastName", SchemaBuilder.string().name("lastName").build())
            .field("ageStr", SchemaBuilder.string().name("ageStr").optional().build())
            .build();

    private final static Schema EVOLVED_VALUE_SCHEMA = SchemaBuilder.struct()
            .name("keySchema")
            .field("firstName", SchemaBuilder.string().name("firstName").build())
            .field("lastName", SchemaBuilder.string().name("lastName").build())
            .field("ageStr", SchemaBuilder.string().name("ageStr").optional().build())
            .field("address", SchemaBuilder.string().name("address").optional().build())
            .build();
    private Random random = new Random();


    private SinkRecord createSinkRecord(Schema keySchema, Struct key, Schema valueSchema, Struct value, long kafkaOffset){
        return new SinkRecord(TOPIC, PARTITION, keySchema, key, valueSchema, value, kafkaOffset);
    }

    private Struct createStruct(Schema schema, Map<String, ?> record){
        Struct struct = new Struct(schema);
        record.forEach(struct::put);
        struct.validate();
        return struct;
    }

    private List<SinkRecord> generateMessages(Schema valueSchema, int numMessages){
        return IntStream.range(0, numMessages).mapToObj(i -> {

            Struct keyStruct = createStruct(KEY_SCHEMA, Map.of("id", String.format("id-%d", i)));

            Map<String, String> valueMap = new HashMap<>();
            valueSchema.fields().forEach(field -> {
                valueMap.put(field.name(), String.format("%s-%d", field.name(), i));
            });
            Struct valueStruct = createStruct(valueSchema, valueMap);

            random = new Random();
            return createSinkRecord(KEY_SCHEMA, keyStruct, valueSchema, valueStruct, random.nextLong());
        }).collect(Collectors.toList());
    }

    private  static <T> List<T>[] split(List<T> list){
        int midIndex = (list.size() - 1) / 2;

        List<List<T>> lists = new ArrayList<>(
                list.stream()
                        .collect(Collectors.partitioningBy(s -> list.indexOf(s) > midIndex))
                        .values()
        );

        // return an array containing both lists
        return new List[] {lists.get(0), lists.get(1)};
    }



    @Test
    void addAllCreatesTable() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);

        List<SinkRecord> messages = generateMessages(EARLY_VALUE_SCHEMA, 10);
        List<SinkRecord>[] messageGroups = split(messages);


        RecordBuffer buffer = new RecordBuffer(genConfig(), tableManager, connection);

        List<SinkRecord> flushedRecords1 = buffer.addAll(messageGroups[0]);// Adding initial messages and creating table
        assertEquals(flushedRecords1.size(), 0);
        verify(tableManager, times(1)).createOrAmendTable(eq(connection), any(KafkaFieldsMetadata.class));
        String expectedSQL = "INSERT INTO (\n" +
                "ID,\n" +
                "FIRSTNAME,\n" +
                "LASTNAME,\n" +
                "AGESTR) SELECT ?,?,?,?\n";
        verify(connection).prepareStatement(eq(expectedSQL));

        List<SinkRecord> flushedRecords2 = buffer.addAll(messageGroups[1]);// Adding more messages, ensuring that records buffer
        assertEquals(flushedRecords2.size(), 0);
        verify(tableManager, times(1)).createOrAmendTable(eq(connection), any(KafkaFieldsMetadata.class));
    }

    @Test
    void addAllAmendsTableOnSchemaChange() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{Statement.SUCCESS_NO_INFO});

        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        List<SinkRecord> earlyMessages = generateMessages(EARLY_VALUE_SCHEMA, 10);
        List<SinkRecord> evolvedMessages = generateMessages(EVOLVED_VALUE_SCHEMA, 20);


        RecordBuffer buffer = new RecordBuffer(genConfig(), tableManager, connection);
        List<SinkRecord> flushedMessages = buffer.addAll(earlyMessages);
        assertEquals(flushedMessages.size(), 0);
        verify(tableManager, times(1)).createOrAmendTable(eq(connection), any(KafkaFieldsMetadata.class));

        List<SinkRecord> afterEvolveFlushedMsgs = buffer.addAll(evolvedMessages);
        assertEquals(afterEvolveFlushedMsgs.size(), earlyMessages.size());
        verify(tableManager, times(2)).createOrAmendTable(eq(connection), any(KafkaFieldsMetadata.class));
        String expectedInsertSQL = "INSERT INTO (\n" +
                "ID,\n" +
                "FIRSTNAME,\n" +
                "LASTNAME,\n" +
                "AGESTR,\n" +
                "ADDRESS) SELECT ?,?,?,?,?\n";
        verify(connection).prepareStatement(eq(expectedInsertSQL));
    }

    @Test
    void addAllFlushesWhenRecordsOverBatchSize() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{Statement.SUCCESS_NO_INFO});

        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        List<SinkRecord> earlyMessages = generateMessages(EARLY_VALUE_SCHEMA, 10);
        List<SinkRecord> earlyMessagesSecondBatch = generateMessages(EARLY_VALUE_SCHEMA, 100);


        RecordBuffer buffer = new RecordBuffer(genConfig(Map.of(BATCH_SIZE, 50)), tableManager, connection);
        List<SinkRecord> flushedMessages = buffer.addAll(earlyMessages);
        assertEquals(flushedMessages.size(), 0);
        verify(preparedStatement, times(0)).executeBatch();

        // Expecting 2 flushes of 50 to happen. Will return 100 flushed records in one call
        List<SinkRecord> flushedMessagesSecondBatch = buffer.addAll(earlyMessagesSecondBatch);
        assertEquals(flushedMessagesSecondBatch.size(), 100);
        verify(preparedStatement, times(2)).executeBatch();

    }



    @Test
    void flushDoesNothingIfNoRecords() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);
        RecordBuffer buffer = new RecordBuffer(genConfig(), tableManager, connection);
        List<SinkRecord> flushedRecords = buffer.flush();
        assertEquals(flushedRecords.size(), 0);
    }


    @Test
    void flushErrsIfExecuteFails() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{Statement.EXECUTE_FAILED});

        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        List<SinkRecord> earlyMessages = generateMessages(EARLY_VALUE_SCHEMA, 10);


        RecordBuffer buffer = new RecordBuffer(genConfig(), tableManager, connection);
        List<SinkRecord> flushedMessages = buffer.addAll(earlyMessages);
        assertEquals(flushedMessages.size(), 0);

        assertThrows(BatchUpdateException.class,
                () -> buffer.flush(),
                "Should throw error if the insert operation failed.");

    }

    @Test
    void closeDoesNothingIfNoRecordsPresent() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{Statement.SUCCESS_NO_INFO});

        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        RecordBuffer buffer = new RecordBuffer(genConfig(), tableManager, connection);
        buffer.close();

        verify(preparedStatement, never()).close();
    }

    @Test
    void close() throws SQLException {
        TableManager tableManager = mock(TableManager.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.executeBatch()).thenReturn(new int[]{Statement.SUCCESS_NO_INFO});

        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        List<SinkRecord> earlyMessages = generateMessages(EARLY_VALUE_SCHEMA, 10);


        RecordBuffer buffer = new RecordBuffer(genConfig(), tableManager, connection);
        List<SinkRecord> flushedMessages = buffer.addAll(earlyMessages);
        assertEquals(flushedMessages.size(), 0);

        buffer.close();
        verify(preparedStatement, times(1)).close();
    }
}