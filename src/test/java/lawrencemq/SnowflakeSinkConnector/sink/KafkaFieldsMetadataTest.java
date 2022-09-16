package lawrencemq.SnowflakeSinkConnector.sink;

import lawrencemq.SnowflakeSinkConnector.sink.exceptions.RecordKeyTypeException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class KafkaFieldsMetadataTest {


    private static String TEST_TABLE_NAME = "\"db1\".\"schema2\".\"table3\"";
    private static Schema keySchema = SchemaBuilder.struct()
            .name("keySchema")
            .field("id", SchemaBuilder.STRING_SCHEMA)
            .build();
    private static Schema valueSchema = SchemaBuilder.struct()
            .name("valueSchema")
            .field("something", SchemaBuilder.string().build())
            .field("else", SchemaBuilder.OPTIONAL_INT16_SCHEMA)
            .field("entirely", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private static Map<String, Schema> getMapOf(Schema... allSchemas) {
        return Arrays.stream(allSchemas)
                .map(Schema::fields)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Field::name, Field::schema));
    }

    private static Set<String> getFieldNamesFor(Schema schema) {
        return schema.fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.toSet());
    }

    @Test
    void testGetters() {
        Set<String> keyFields = getFieldNamesFor(keySchema);
        Set<String> valueFields = getFieldNamesFor(valueSchema);
        Map<String, Schema> fieldsToSchemaMap = getMapOf(keySchema, valueSchema);
        KafkaFieldsMetadata metadata = new KafkaFieldsMetadata(keyFields, valueFields, fieldsToSchemaMap);

        assertSame(keyFields, metadata.getKeyFields());
        assertSame(valueFields, metadata.getValueFields());
        assertSame(fieldsToSchemaMap, metadata.getAllFields());
        assertEquals(keyFields.size() + valueFields.size(), metadata.getAllFieldNames().size());
    }


    @Test
    void extract() {
        TopicSchemas topicSchemas = new TopicSchemas(keySchema, valueSchema);
        KafkaFieldsMetadata metadata = KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas);

        assertEquals(metadata.getAllFields().size(), 4);
        assertEquals(new HashSet<>(metadata.getAllFieldNames()), getMapOf(keySchema, valueSchema).keySet());
        assertEquals(metadata.getKeyFields(), getFieldNamesFor(keySchema));
        assertEquals(metadata.getValueFields(), getFieldNamesFor(valueSchema));
    }

    @Test
    void extractWithNullKey() {
        TopicSchemas topicSchemas = new TopicSchemas(null, valueSchema);
        KafkaFieldsMetadata metadata = KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas);

        assertEquals(metadata.getAllFields().size(), 3);
        assertEquals(new HashSet<>(metadata.getAllFieldNames()), getMapOf(valueSchema).keySet());
        assertEquals(metadata.getKeyFields(), Set.of());
        assertEquals(metadata.getValueFields(), getFieldNamesFor(valueSchema));
    }


    @Test
    void extractWithPrimitiveKey() {
        TopicSchemas topicSchemas = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, valueSchema);
        assertThrows(RecordKeyTypeException.class,
                () -> KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas),
                "ensures key is null or struct");
    }

    @Test
    void extractErrorsWithValueNonStruct() {
        TopicSchemas topicSchemas = new TopicSchemas(keySchema, Schema.STRING_SCHEMA);
        assertThrows(ConnectException.class,
                () -> KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas),
                "Ensures value schema is a struct.");
    }

    @Test
    void extractErrorsWithValueStructNull() {
        TopicSchemas topicSchemas = new TopicSchemas(keySchema, null);
        assertThrows(ConnectException.class,
                () -> KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas),
                "Ensures value schema is not null.");
    }

    @Test
    void extractErrorsCannotFindFields() {
        Schema emptyStructSchema = SchemaBuilder.struct().name("basicStruct").build();
        TopicSchemas topicSchemas = new TopicSchemas(emptyStructSchema, emptyStructSchema);

        assertThrows(ConnectException.class,
                () -> KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas),
                "Ensures fields from structs exist");
    }

    @Test
    void extractErrorsFromOverlappingNamedFields() {
        TopicSchemas topicSchemas = new TopicSchemas(
                SchemaBuilder.struct()
                        .name("keyStruct")
                        .field("a", SchemaBuilder.STRING_SCHEMA)
                        .build(),
                SchemaBuilder.struct()
                        .name("valueStruct")
                        .field("a", SchemaBuilder.STRING_SCHEMA)
                        .field("b", SchemaBuilder.STRING_SCHEMA)
                        .build());

        // TODO, THIS ISN'T CURRENTLY FAILING. SHOULD IT? DID I MAKE AMISTAKE AND CREATE A BUG?
        assertThrows(IllegalArgumentException.class,
                () -> KafkaFieldsMetadata.from(TEST_TABLE_NAME, topicSchemas),
                "Ensures fields between key and value do not overlap");
    }


    @Test
    void testToString() {
        Set<String> keyFields = getFieldNamesFor(keySchema);
        Set<String> valueFields = getFieldNamesFor(valueSchema);
        Map<String, Schema> fieldsToSchemaMap = getMapOf(keySchema, valueSchema);
        KafkaFieldsMetadata metadata = new KafkaFieldsMetadata(keyFields, valueFields, fieldsToSchemaMap);
        assertEquals(metadata.toString(), "KafkaFieldsMetadata{keyFields=[id],valueFields=[else, entirely, something]}");
    }
}