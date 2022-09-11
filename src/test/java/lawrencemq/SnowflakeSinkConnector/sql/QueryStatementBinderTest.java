package lawrencemq.SnowflakeSinkConnector.sql;

import lawrencemq.SnowflakeSinkConnector.sink.KafkaFieldsMetadata;
import lawrencemq.SnowflakeSinkConnector.sink.TopicSchemas;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class QueryStatementBinderTest {

    private static final String topic = "TEST_TOPIC";
    private static final int partition = 1;
    private static final long kafkaOffset = 123456789L;
    private static final Schema singleFiledStructSchema = SchemaBuilder.struct().name("person")
            .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema valueSchema = SchemaBuilder.struct().name("structValue")
            .field("str", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("byte", Schema.INT8_SCHEMA)
            .field("short", Schema.INT16_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .field("long", Schema.INT64_SCHEMA)
            .field("float", Schema.FLOAT32_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("bool", Schema.BOOLEAN_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA))
            .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA))
            .field("struct", singleFiledStructSchema)
            .field("decimal", Decimal.schema(5))
            .field("date", Date.SCHEMA)
            .field("time", Time.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .field("optional", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();


    private static final Struct singleFiledStructValue = new Struct(singleFiledStructSchema)
            .put("firstName", "Testy")
            .put("lastName", "McTestFace");
    private static final java.util.Date datetime = new java.util.Date(1662767585000L);
    private static final Struct value = new Struct(valueSchema)
            .put("str", "BenderIsGreat")
            .put("bytes", new byte[]{-64, 64})
            .put("byte", (byte) 16)
            .put("short", (short) 25)
            .put("int", 42)
            .put("long", (long) 123456789L)
            .put("float", (float) 42.690)
            .put("double", 42.691)
            .put("bool", true)
            .put("array", List.of("a", "b", "c"))
            .put("map", Map.of(1, 2))
            .put("struct", singleFiledStructValue)
            .put("decimal", new BigDecimal(5.2))
            .put("date", datetime)
            .put("time", datetime)
            .put("timestamp", datetime);

    Set<String> valueFields = schemaToFieldsSet(valueSchema);
    Map<String, Schema> valueMap = schemaToFieldsMap(valueSchema);

    private static Set<String> schemaToFieldsSet(Schema schema) {
        return schema.fields().stream().map(Field::name).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static Map<String, Schema> schemaToFieldsMap(Schema schema) {
        return schema.fields().stream().collect(Collectors.toMap(Field::name, Field::schema));
    }

    private static <K, V> Map<K, V> unionMaps(Map<K, V> m1, Map<K, V> m2) {
        Map<K, V> allFieldsMap = new HashMap<>(m1);
        allFieldsMap.putAll(m2);
        return allFieldsMap;
    }


    @Test
    void bindPrimitiveKey() {
        Schema keySchema = SchemaBuilder.int32().name("int32Key").defaultValue(12).build();

        TopicSchemas topicSchemas = new TopicSchemas(keySchema, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(keySchema.name()), Set.of(valueSchema.name()), Map.of(keySchema.name(), keySchema, valueSchema.name(), valueSchema));
        SinkRecord record = new SinkRecord(topic, partition, keySchema, 42, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        assertThrows(AssertionError.class, () -> binder.bind(record), "Should error if key is a primitive type");
    }

    @Test
    void bindNullKey() throws SQLException {
        TopicSchemas topicSchemas = new TopicSchemas(null, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), valueFields, valueMap);
        SinkRecord record = new SinkRecord(topic, partition, null, null, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record);
        verify(statement).addBatch();

        // todo verify more?
    }

    @Test
    void bindStructKey() throws SQLException {
        Schema keySchema = SchemaBuilder.struct().name("structKey")
                .field("firstName", Schema.STRING_SCHEMA)
                .field("lastName", Schema.STRING_SCHEMA)
                .build();

        Struct keyStruct = new Struct(keySchema)
                .put("firstName", "Phillip")
                .put("lastName", "Fry");

        Set<String> keyFields = schemaToFieldsSet(keySchema);
        Map<String, Schema> keyMap = schemaToFieldsMap(keySchema);
        Map<String, Schema> allFieldsMap = unionMaps(valueMap, keyMap);

        TopicSchemas topicSchemas = new TopicSchemas(keySchema, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(keyFields, valueFields, allFieldsMap);
        SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record);
        verify(statement).addBatch();

        // todo verify more?

    }

    @Test
    void bindNullValue() throws SQLException {
        Schema valueSchema = SchemaBuilder.struct().name("structValue")
                .field("str", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema)
                .put("str", null);

        TopicSchemas topicSchemas = new TopicSchemas(null, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(valueSchema), schemaToFieldsMap(valueSchema));
        SinkRecord record = new SinkRecord(topic, partition, null, null, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record);
        verify(statement).setObject(eq(1), eq(null));
        verify(statement).addBatch();
    }

    @Test
    void bindPrimitiveValue() throws SQLException {
        TopicSchemas topicSchemas = new TopicSchemas(null, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(valueSchema), schemaToFieldsMap(valueSchema));
        SinkRecord record = new SinkRecord(topic, partition, null, null, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record);
        verify(statement).setString(eq(1), eq("BenderIsGreat"));
        verify(statement).setBytes(eq(2), eq(new byte[]{-64, 64}));
        verify(statement).setByte(eq(3), eq((byte) 16));
        verify(statement).setShort(eq(4), eq((short) 25));
        verify(statement).setInt(eq(5), eq(42));
        verify(statement).setLong(eq(6), eq(123456789L));
        verify(statement).setFloat(eq(7), eq((float) 42.690));
        verify(statement).setDouble(eq(8), eq(42.691));
        verify(statement).setBoolean(eq(9), eq(true));
        verify(statement).setString(eq(10), eq("[\"a\",\"b\",\"c\"]"));
        verify(statement).setString(eq(11), eq("{\"1\":2}"));
        verify(statement).setString(eq(12), eq("{\"schema\":{\"type\":\"STRUCT\",\"optional\":false,\"fields\":[{\"name\":\"firstName\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"optional\":true}},{\"name\":\"lastName\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"optional\":true}}],\"fieldsByName\":{\"firstName\":{\"name\":\"firstName\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"optional\":true}},\"lastName\":{\"name\":\"lastName\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"optional\":true}}},\"name\":\"person\"},\"values\":[\"Testy\",\"McTestFace\"]}"));
        verify(statement).addBatch();

    }

    @Test
    void bindComplexValue() throws SQLException {
        TopicSchemas topicSchemas = new TopicSchemas(null, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(valueSchema), schemaToFieldsMap(valueSchema));
        SinkRecord record = new SinkRecord(topic, partition, null, null, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record);
        verify(statement).setBigDecimal(eq(13), eq(new BigDecimal(5.2)));
        verify(statement).setDate(eq(14), eq(new java.sql.Date((datetime).getTime())), any(Calendar.class));
        verify(statement).setTime(eq(15), eq(new java.sql.Time((datetime).getTime())), any(Calendar.class));
        verify(statement).setTimestamp(eq(16), eq(new java.sql.Timestamp((datetime).getTime())), any(Calendar.class));
        verify(statement).addBatch();
    }
}