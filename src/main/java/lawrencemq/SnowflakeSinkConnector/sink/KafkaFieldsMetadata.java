package lawrencemq.SnowflakeSinkConnector.sink;


import lawrencemq.SnowflakeSinkConnector.sink.exceptions.RecordKeyTypeException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


public class KafkaFieldsMetadata {

    private final Set<String> keyFields;
    private final Set<String> valueFields;
    private final Map<String, Schema> allFields;

    public KafkaFieldsMetadata(Set<String> keyFields, Set<String> valueFields, Map<String, Schema> allFields) {
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

    public static KafkaFieldsMetadata extract(String tableName, TopicSchemas topicSchemas) {
        Schema keySchema = topicSchemas.keySchema();
        Schema valueSchema = topicSchemas.valueSchema();

        if (Objects.nonNull(valueSchema) && valueSchema.type() != Schema.Type.STRUCT) {
            throw new ConnectException("Value schema must be of type Struct");
        }

        Map<String, Schema> allFields = new HashMap<>();
        Set<String> keyFieldNames = extractKeyFields(keySchema, allFields);

        Set<String> nonKeyFieldNames = new LinkedHashSet<>();
        if (Objects.nonNull(valueSchema)) {
            valueSchema.fields().stream()
                    .filter(field -> !keyFieldNames.contains(field.name()))
                    .forEach(field -> {
                        nonKeyFieldNames.add(field.name());
                        allFields.put(field.name(), field.schema());
                    });
        }

        if (allFields.isEmpty()) {
            throw new ConnectException(
                    "No fields found using key and value schemas for table: " + tableName
            );
        }

        LinkedHashMap<String, Schema> allFieldsOrdered = new LinkedHashMap<>();

        if (Objects.nonNull(valueSchema)) {
            for (Field field : valueSchema.fields()) {
                String fieldName = field.name();
                if (allFields.containsKey(fieldName)) {
                    allFieldsOrdered.put(fieldName, allFields.get(fieldName));
                }
            }
        }

        if (allFieldsOrdered.size() < allFields.size()) {
            ArrayList<String> fieldKeys = new ArrayList<>(allFields.keySet());
            Collections.sort(fieldKeys);
            fieldKeys.stream()
                    .filter(fieldName -> !allFieldsOrdered.containsKey(fieldName))
                    .forEach(fieldName -> allFieldsOrdered.put(fieldName, allFields.get(fieldName)));
        }

        ensureInclusiveFields(keyFieldNames, nonKeyFieldNames, allFields, allFieldsOrdered);

        return new KafkaFieldsMetadata(keyFieldNames, nonKeyFieldNames, allFieldsOrdered);
    }

    private static void ensureInclusiveFields(Set<String> keyFieldNames, Set<String> nonKeyFieldNames, Map<String, Schema> allFields, LinkedHashMap<String, Schema> allFieldsOrdered) {
        boolean fieldCountsMatch = keyFieldNames.size() + nonKeyFieldNames.size() == allFields.size();
        boolean allFieldsContained = allFieldsOrdered.keySet().containsAll(keyFieldNames)
                && allFieldsOrdered.keySet().containsAll(nonKeyFieldNames);
        if (!fieldCountsMatch || !allFieldsContained) {
            throw new IllegalArgumentException(String.format(
                    "Validation fail -- keyFieldNames:%s nonKeyFieldNames:%s allFields:%s",
                    keyFieldNames, nonKeyFieldNames, allFieldsOrdered
            ));
        }
    }


    private static Set<String> extractKeyFields(Schema keySchema, Map<String, Schema> allFields) {
        Set<String> keyFieldNames = new LinkedHashSet<>();
        if (Objects.isNull(keySchema)) {
            return keyFieldNames;
        }
        Schema.Type keySchemaType = keySchema.type();
        if (keySchemaType == Schema.Type.STRUCT) {
            keySchema.fields().stream()
                    .map(Field::name)
                    .forEach(keyFieldNames::add);

            keyFieldNames.forEach(fieldName ->
                allFields.put(fieldName, keySchema.field(fieldName).schema())
            );
        } else {
            throw new RecordKeyTypeException("Key schema must null or Struct, but is of type: " + keySchemaType);
        }
        return keyFieldNames;
    }

    public List<String> getAllFieldNames(){
        List<String> allFields = new ArrayList<>(keyFields);
        allFields.addAll(valueFields);
        return allFields;
    }

    public Map<String, Schema> getAllFields() {
        return allFields;
    }

    @Override
    public String toString() {
        return String.format("KafkaFieldsMetadata{keyFields=%s,valueFields=%s}", keyFields, valueFields);
    }
}