package lawrencemq.SnowflakeJdbcSinkConnector.sink;

import org.apache.kafka.connect.data.*;

import java.util.*;

public class KafkaMetadata {

    public enum MetadataField {
        TOPIC("_KAFKA_TOPIC"),
        PARTITION("_KAFKA_PARTITION"),
        OFFSET("_KAFKA_OFFSET"),
        TIMESTAMP("_KAFKA_TIMESTAMP");

        private String fieldName;

        MetadataField(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return this.fieldName;
        }

        @Override
        public String toString(){
            return this.fieldName;
        }

        public static Optional<MetadataField> fromText(String text) {
            return Arrays.stream(values())
                    .filter(bl -> bl.fieldName.equalsIgnoreCase(text))
                    .findFirst();
        }
    }

    public static final Schema KAFKA_METADATA_SCHEMA = SchemaBuilder.struct()
            .field(MetadataField.TOPIC.toString(), Schema.OPTIONAL_STRING_SCHEMA)
            .field(MetadataField.PARTITION.toString(), Schema.OPTIONAL_INT32_SCHEMA)
            .field(MetadataField.OFFSET.toString(), Schema.OPTIONAL_INT64_SCHEMA)
            .field(MetadataField.TIMESTAMP.toString(), Schema.OPTIONAL_INT64_SCHEMA)
            .build();

    public static Map<MetadataField, Field> getKafkaMetadataFieldToSchemaMap() {
        Map<MetadataField, Field> fieldToSchemaMap = new LinkedHashMap<>();

        KAFKA_METADATA_SCHEMA.fields()
                .forEach(field -> fieldToSchemaMap.put(MetadataField.fromText(field.name()).orElseThrow(), field));

        return fieldToSchemaMap;
    }

    public static List<MetadataField> getKafkaMetadataFields(){
        return List.of(
                MetadataField.TOPIC,
                MetadataField.PARTITION,
                MetadataField.OFFSET,
                MetadataField.TIMESTAMP
        );
    }

}
