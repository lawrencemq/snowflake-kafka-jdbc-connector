package lawrencemq.SnowflakeJdbcSinkConnector.sink;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

import static lawrencemq.SnowflakeJdbcSinkConnector.sink.KafkaMetadata.KAFKA_METADATA_SCHEMA;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.KafkaMetadata.getKafkaMetadataFields;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaFieldsMetadataTest {


    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("keySchema")
            .field("id", SchemaBuilder.STRING_SCHEMA)
            .build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("valueSchema")
            .field("something", SchemaBuilder.string().build())
            .field("else", SchemaBuilder.OPTIONAL_INT16_SCHEMA)
            .field("entirely", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    private static LinkedHashMap<String, Field> getMapOf(Schema... allSchemas) {
        LinkedHashMap<String, Field> allFields = new LinkedHashMap<>();
        Arrays.stream(allSchemas)
                .map(Schema::fields)
                .flatMap(Collection::stream)
                .forEach(field -> allFields.put(field.name(), field));
        return allFields;
    }

    private static Set<String> getFieldNamesFor(Schema schema) {
        return schema.fields()
                .stream()
                .map(Field::name)
                .collect(Collectors.toSet());
    }

    @Test
    void testGetters() {
        Set<String> keyFields = getFieldNamesFor(KEY_SCHEMA);
        Set<String> valueFields = getFieldNamesFor(VALUE_SCHEMA);
        LinkedHashMap<String, Field> fieldsToSchemaMap = getMapOf(KEY_SCHEMA, VALUE_SCHEMA);
        KafkaFieldsMetadata metadata = new KafkaFieldsMetadata(keyFields, valueFields, getKafkaMetadataFields(), fieldsToSchemaMap);

        assertSame(keyFields, metadata.getKeyFields());
        assertSame(valueFields, metadata.getValueFields());
        assertSame(fieldsToSchemaMap, metadata.getAllFields());
        assertEquals(keyFields.size() + valueFields.size() + getKafkaMetadataFields().size(), metadata.getAllFieldNames().size());
    }


    @Test
    void extract() {
        TopicSchemas topicSchemas = new TopicSchemas(KEY_SCHEMA, VALUE_SCHEMA);
        KafkaFieldsMetadata metadata = KafkaFieldsMetadata.from(topicSchemas, true);

        assertEquals(metadata.getAllFields().size(), 8);
        assertEquals(new HashSet<>(metadata.getAllFieldNames()), getMapOf(KEY_SCHEMA, VALUE_SCHEMA, KAFKA_METADATA_SCHEMA).keySet());
        assertEquals(metadata.getKeyFields(), getFieldNamesFor(KEY_SCHEMA));
        assertEquals(metadata.getValueFields(), getFieldNamesFor(VALUE_SCHEMA));
        assertEquals(metadata.getKafkaMetadataFields(), getKafkaMetadataFields());
    }

    @Test
    void extractWithNullKey() {
        TopicSchemas topicSchemas = new TopicSchemas(null, VALUE_SCHEMA);
        KafkaFieldsMetadata metadata = KafkaFieldsMetadata.from(topicSchemas, false);

        assertEquals(metadata.getAllFields().size(), 3);
        assertEquals(new HashSet<>(metadata.getAllFieldNames()), getMapOf(VALUE_SCHEMA).keySet());
        assertEquals(metadata.getKeyFields(), Set.of());
        assertEquals(metadata.getValueFields(), getFieldNamesFor(VALUE_SCHEMA));
    }

    @Test
    void extractWithNullKeyIncludeKafkaMetadata(){
        TopicSchemas topicSchemas = new TopicSchemas(null, VALUE_SCHEMA);
        KafkaFieldsMetadata metadata = KafkaFieldsMetadata.from(topicSchemas, true);

        assertEquals(metadata.getAllFields().size(), 7);
        assertEquals(new HashSet<>(metadata.getAllFieldNames()), getMapOf(VALUE_SCHEMA, KAFKA_METADATA_SCHEMA).keySet());
        assertEquals(metadata.getKeyFields(), Set.of());
        assertEquals(metadata.getValueFields(), getFieldNamesFor(VALUE_SCHEMA));
        assertEquals(metadata.getKafkaMetadataFields(), getKafkaMetadataFields());
    }


    @Test
    void extractWithPrimitiveKey() {
        TopicSchemas topicSchemas = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, VALUE_SCHEMA);
        assertThrows(RecordKeyTypeException.class,
                () -> KafkaFieldsMetadata.from(topicSchemas, false),
                "ensures key is null or struct");
    }

    @Test
    void extractErrorsWithValueNonStruct() {
        TopicSchemas topicSchemas = new TopicSchemas(KEY_SCHEMA, Schema.STRING_SCHEMA);
        assertThrows(RecordValueTypeException.class,
                () -> KafkaFieldsMetadata.from(topicSchemas, false),
                "Ensures value schema is a struct.");
    }

    @Test
    void extractErrorsWithValueStructNull() {
        TopicSchemas topicSchemas = new TopicSchemas(KEY_SCHEMA, null);
        assertThrows(RecordValueTypeException.class,
                () -> KafkaFieldsMetadata.from(topicSchemas, false),
                "Ensures value schema is not null.");
    }

    @Test
    void extractErrorsCannotFindFields() {
        Schema emptyStructSchema = SchemaBuilder.struct().name("basicStruct").build();
        TopicSchemas topicSchemas = new TopicSchemas(emptyStructSchema, emptyStructSchema);

        assertThrows(InvalidColumnsError.class,
                () -> KafkaFieldsMetadata.from(topicSchemas, false),
                "Ensures key and value has at least one field");
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

        assertThrows(InvalidColumnsError.class,
                () -> KafkaFieldsMetadata.from(topicSchemas, false),
                "Ensures fields between key and value do not overlap");
    }


    @Test
    void testToString() {
        Set<String> keyFields = getFieldNamesFor(KEY_SCHEMA);
        Set<String> valueFields = getFieldNamesFor(VALUE_SCHEMA);
        LinkedHashMap<String, Field> fieldsToSchemaMap = getMapOf(KEY_SCHEMA, VALUE_SCHEMA);
        KafkaFieldsMetadata metadata = new KafkaFieldsMetadata(keyFields, valueFields, getKafkaMetadataFields(), fieldsToSchemaMap);
        assertEquals(metadata.toString(), "KafkaFieldsMetadata{keyFields=[id],valueFields=[else, entirely, something]}");
    }
}