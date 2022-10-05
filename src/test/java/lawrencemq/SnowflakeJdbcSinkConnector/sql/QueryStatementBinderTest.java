package lawrencemq.SnowflakeJdbcSinkConnector.sql;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.KafkaFieldsMetadata;
import lawrencemq.SnowflakeJdbcSinkConnector.sink.TopicSchemas;
import org.apache.kafka.common.record.*;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static lawrencemq.SnowflakeJdbcSinkConnector.sink.KafkaMetadata.getKafkaMetadataFields;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class QueryStatementBinderTest {

    private static final String topic = "TEST_TOPIC";
    private static final int partition = 1;
    private static final long kafkaOffset = 123456789L;
    private static final Schema SINGLE_FILED_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
            .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
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
            .field("struct", SINGLE_FILED_STRUCT_SCHEMA)
            .field("decimal", Decimal.schema(5))
            .field("date", Date.SCHEMA)
            .field("time", Time.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .field("optional", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .build();

    private static final Field VALUE_FIELD = new Field("structValue", 0, VALUE_SCHEMA);


    private static final Struct SINGLE_FILED_STRUCT_VALUE = new Struct(SINGLE_FILED_STRUCT_SCHEMA)
            .put("firstName", "Testy")
            .put("lastName", "McTestFace");
    private static final java.util.Date DATETIME = new java.util.Date(1662767585000L);
    private static final Struct VALUE = new Struct(VALUE_SCHEMA)
            .put("str", "BenderIsGreat")
            .put("bytes", new byte[]{-64, 64})
            .put("byte", (byte) 16)
            .put("short", (short) 25)
            .put("int", 42)
            .put("long", 123456789L)
            .put("float", (float) 42.690)
            .put("double", 42.691)
            .put("bool", true)
            .put("array", List.of("a", "b", "c"))
            .put("map", Map.of(1, 2))
            .put("struct", SINGLE_FILED_STRUCT_VALUE)
            .put("decimal", new BigDecimal("5.2"))
            .put("date", DATETIME)
            .put("time", DATETIME)
            .put("timestamp", DATETIME);

    final Set<String> valueFields = schemaToFieldsSet(VALUE_SCHEMA);
    final LinkedHashMap<String, Field> valueMap = schemaToFieldsMap(VALUE_SCHEMA);

    private static Set<String> schemaToFieldsSet(Schema schema) {
        return schema.fields().stream().map(Field::name).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static LinkedHashMap<String, Field> schemaToFieldsMap(Schema schema) {
        LinkedHashMap<String, Field> schemaToFields = new LinkedHashMap<>();
        schema.fields().forEach(field -> schemaToFields.put(field.name(), field));
        return schemaToFields;
    }

    private static <K, V> LinkedHashMap<K, V> unionMaps(LinkedHashMap<K, V> m1, LinkedHashMap<K, V> m2) {
        LinkedHashMap<K, V> allFieldsMap = new LinkedHashMap<>(m1);
        allFieldsMap.putAll(m2);
        return allFieldsMap;
    }


    @Test
    void bindPrimitiveKey() {
        Schema keySchema = SchemaBuilder.int32().defaultValue(12).build();
        LinkedHashMap<String,Field> fieldToSchemaMap = new LinkedHashMap<>();
        String keyFieldName = "int32key";
        fieldToSchemaMap.put(keySchema.name(), new Field(keyFieldName, 0, keySchema));
        fieldToSchemaMap.put(VALUE_FIELD.name(), VALUE_FIELD);

        TopicSchemas topicSchemas = new TopicSchemas(keySchema, VALUE_SCHEMA);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(keyFieldName), Set.of(VALUE_FIELD.name()), getKafkaMetadataFields(), fieldToSchemaMap);
        SinkRecord record = new SinkRecord(topic, partition, keySchema, 42, VALUE_SCHEMA, VALUE, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        assertThrows(AssertionError.class, () -> binder.bind(record, true), "Should error if key is a primitive type");
    }

    @Test
    void bindNullKey() throws SQLException {
        TopicSchemas topicSchemas = new TopicSchemas(null, VALUE_SCHEMA);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), valueFields, getKafkaMetadataFields(), valueMap);
        SinkRecord record = new SinkRecord(topic, partition, null, null, VALUE_SCHEMA, VALUE, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record, true);
        verify(statement).addBatch();
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
        LinkedHashMap<String, Field> keyMap = schemaToFieldsMap(keySchema);
        LinkedHashMap<String, Field> allFieldsMap = unionMaps(valueMap, keyMap);

        TopicSchemas topicSchemas = new TopicSchemas(keySchema, VALUE_SCHEMA);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(keyFields, valueFields, getKafkaMetadataFields(), allFieldsMap);
        SinkRecord record = new SinkRecord(topic, partition, keySchema, keyStruct, VALUE_SCHEMA, VALUE, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record, true);
        verify(statement).addBatch();
        verify(statement).setString(eq(1), eq("Phillip"));
        verify(statement).setString(eq(2), eq("Fry"));
    }

    @Test
    void bindNullValue() throws SQLException {
        Schema valueSchema = SchemaBuilder.struct().name("structValue")
                .field("str", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema)
                .put("str", null);

        TopicSchemas topicSchemas = new TopicSchemas(null, valueSchema);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(valueSchema), getKafkaMetadataFields(), schemaToFieldsMap(valueSchema));
        SinkRecord record = new SinkRecord(topic, partition, null, null, valueSchema, value, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record, true);
        verify(statement).setObject(eq(1), eq(null));
        verify(statement).addBatch();
    }

    @Test
    void bindPrimitiveValue() throws SQLException {
        TopicSchemas topicSchemas = new TopicSchemas(null, VALUE_SCHEMA);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(VALUE_SCHEMA), getKafkaMetadataFields(), schemaToFieldsMap(VALUE_SCHEMA));
        SinkRecord record = new SinkRecord(topic, partition, null, null, VALUE_SCHEMA, VALUE, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record, true);
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
        verify(statement).setString(eq(12), eq("{\"schema\":{\"type\":\"STRUCT\",\"optional\":false,\"fields\":[{\"name\":\"firstName\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"optional\":true}},{\"name\":\"lastName\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"optional\":true}}],\"fieldsByName\":{\"firstName\":{\"name\":\"firstName\",\"index\":0,\"schema\":{\"type\":\"STRING\",\"optional\":true}},\"lastName\":{\"name\":\"lastName\",\"index\":1,\"schema\":{\"type\":\"STRING\",\"optional\":true}}}},\"values\":[\"Testy\",\"McTestFace\"]}"));
        verify(statement).addBatch();

    }

    @Test
    void bindComplexValue() throws SQLException {
        TopicSchemas topicSchemas = new TopicSchemas(null, VALUE_SCHEMA);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(VALUE_SCHEMA), getKafkaMetadataFields(), schemaToFieldsMap(VALUE_SCHEMA));
        SinkRecord record = new SinkRecord(topic, partition, null, null, VALUE_SCHEMA, VALUE, kafkaOffset);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record, true);
        verify(statement).setBigDecimal(eq(13), eq(new BigDecimal("5.2")));
        verify(statement).setDate(eq(14), eq(new java.sql.Date((DATETIME).getTime())), any(Calendar.class));
        verify(statement).setTime(eq(15), eq(new java.sql.Time((DATETIME).getTime())), any(Calendar.class));
        verify(statement).setTimestamp(eq(16), eq(new java.sql.Timestamp((DATETIME).getTime())), any(Calendar.class));
        verify(statement).setObject(eq(17), eq(null));
        verify(statement).addBatch();
    }

    @Test
    void bindKafkaMetadata() throws SQLException {
        long timestamp = 123457890L;
        TopicSchemas topicSchemas = new TopicSchemas(null, VALUE_SCHEMA);
        KafkaFieldsMetadata kafkaFieldsMetadata = new KafkaFieldsMetadata(Set.of(), schemaToFieldsSet(VALUE_SCHEMA), getKafkaMetadataFields(), schemaToFieldsMap(VALUE_SCHEMA));
        SinkRecord record = new SinkRecord(topic, partition, null, null, VALUE_SCHEMA, VALUE, kafkaOffset, timestamp, TimestampType.CREATE_TIME);
        PreparedStatement statement = mock(PreparedStatement.class);

        QueryStatementBinder binder = new QueryStatementBinder(statement, topicSchemas, kafkaFieldsMetadata);
        binder.bind(record, true);
        verify(statement).addBatch();
        verify(statement).setString(eq(18), eq(topic));
        verify(statement).setInt(eq(19), eq(partition));
        verify(statement).setLong(eq(20), eq(kafkaOffset));
        verify(statement).setLong(eq(21), eq(timestamp));
    }
}