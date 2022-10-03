package lawrencemq.SnowflakeJdbcSinkConnector.sink;


import lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions.*;
import org.apache.kafka.connect.data.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class KafkaFieldsMetadata {

    private final Set<String> keyFields;
    private final Set<String> valueFields;
    private final LinkedHashMap<String, Field> allFields;

    public KafkaFieldsMetadata(Set<String> keyFields, Set<String> valueFields, LinkedHashMap<String, Field> allFields) {
        this.keyFields = keyFields;
        this.valueFields = valueFields;
        this.allFields = allFields;
    }

    public Set<String> getKeyFields() {
        return keyFields;
    }

    public Set<String> getValueFields() {
        return valueFields;
    }

    public LinkedHashMap<String, Field> getAllFields() {
        return allFields;
    }

    public List<String> getAllFieldNames() {
        List<String> allFields = new ArrayList<>(keyFields);
        allFields.addAll(valueFields);
        return allFields;
    }


    public static KafkaFieldsMetadata from(TopicSchemas topicSchemas) {
        Schema keySchema = topicSchemas.keySchema();
        Schema valueSchema = topicSchemas.valueSchema();

        if (Objects.isNull(valueSchema) || valueSchema.type() != Schema.Type.STRUCT) {
            throw new RecordValueTypeException("Value schema must be of type Struct");
        }


        if (Objects.nonNull(topicSchemas.keySchema())) {
            Schema.Type type = topicSchemas.keySchema().type();
            if (type != Schema.Type.STRUCT) {
                throw new RecordKeyTypeException("Key schema must null or Struct, but is of type: " + type);
            }
        }


        Map<String, Field> keyFieldToSchemaMap = extractFieldsFromSchema(keySchema);
        Map<String, Field> valueFieldToSchemaMap = extractFieldsFromSchema(valueSchema);

        if (keyFieldToSchemaMap.isEmpty() && valueFieldToSchemaMap.isEmpty()) {
            throw new InvalidColumnsError("Key and value schemas were found to be empty.");
        }

        Set<String> allFieldNames = new HashSet<>(keyFieldToSchemaMap.keySet());
        allFieldNames.retainAll(valueFieldToSchemaMap.keySet());
        if (!allFieldNames.isEmpty()) {
            throw new InvalidColumnsError("Key and Value schemas have overlapping columns: " + String.join(", ", allFieldNames));
        }

        LinkedHashMap<String, Field> allFieldsOrdered = new LinkedHashMap<>();
        allFieldsOrdered.putAll(keyFieldToSchemaMap);
        allFieldsOrdered.putAll(valueFieldToSchemaMap);

        return new KafkaFieldsMetadata(keyFieldToSchemaMap.keySet(), valueFieldToSchemaMap.keySet(), allFieldsOrdered);
    }


    private static Map<String, Field> extractFieldsFromSchema(Schema schema) {
        Map<String, Field> fieldToSchemaMap = new LinkedHashMap<>();
        if (Objects.isNull(schema)) {
            return fieldToSchemaMap;
        }

        schema.fields()
                .forEach(field -> fieldToSchemaMap.put(field.name(), field));

        return fieldToSchemaMap;
    }

    @Override
    public String toString() {
        return String.format("KafkaFieldsMetadata{keyFields=%s,valueFields=%s}", keyFields, valueFields);
    }
}