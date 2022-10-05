package lawrencemq.SnowflakeJdbcSinkConnector.sql;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static lawrencemq.SnowflakeJdbcSinkConnector.sink.KafkaMetadata.getKafkaMetadataFieldToSchemaMap;


public final class QueryStatementBinder {

    private final PreparedStatement statement;
    private final TopicSchemas topicSchemas;
    private final KafkaFieldsMetadata kafkaFieldsMetadata;


    public QueryStatementBinder(PreparedStatement statement, TopicSchemas topicSchemas, KafkaFieldsMetadata kafkaFieldsMetadata) {
        this.statement = statement;
        this.topicSchemas = topicSchemas;
        this.kafkaFieldsMetadata = kafkaFieldsMetadata;
    }

    private static void bindField(PreparedStatement statement, int index, Schema schema, Object value) throws SQLException {
        if (Objects.isNull(value)) {
            statement.setObject(index, null);
            return;
        }

        AvroSnowflakeConverter.bind(statement, index, schema, value);
    }

    public void bind(SinkRecord record, boolean includeKafkaMetadata) throws SQLException {
        Struct valueStruct = (Struct) record.value();

        int columnIndex = 1;
        columnIndex = bindKeyFields(record, columnIndex);
        columnIndex = bindValueFields(record, valueStruct, columnIndex);

        if(includeKafkaMetadata){
            bindKafkaMetadataFields(record, columnIndex);
        }

        statement.addBatch();
    }

    private int bindKafkaMetadataFields(SinkRecord record, int columnIndex) throws SQLException {
        Map<KafkaMetadata.MetadataField, Field> kafkaMetadataFieldToSchemaMap = getKafkaMetadataFieldToSchemaMap();
        for (KafkaMetadata.MetadataField field : kafkaFieldsMetadata.getKafkaMetadataFields()) {
            Field metadataField = kafkaMetadataFieldToSchemaMap.get(field);
            Object value;
            switch (field){
                case TOPIC:
                    value = record.topic();
                    break;
                case PARTITION:
                    value = record.kafkaPartition();
                    break;
                case OFFSET:
                    value = record.kafkaOffset();
                    break;
                case TIMESTAMP:
                    value = record.timestamp();
                    break;
                default:
                    throw new IllegalArgumentException("Unable to bind metadata field: " + field.getFieldName());
            }
            bindField(statement, columnIndex++,  metadataField.schema(), value);
        }
        return columnIndex;
    }

    private int bindKeyFields(SinkRecord record, int columnIndex) throws SQLException {
        if(Objects.isNull(topicSchemas.keySchema())){
            return columnIndex;
        }

        if (topicSchemas.keySchema().type().isPrimitive()) {
            throw new AssertionError("Key schema was primitive - unsupported. Must be null or struct.");
        }
        for (String fieldName : kafkaFieldsMetadata.getKeyFields()) {
            Field field = topicSchemas.keySchema().field(fieldName);
            bindField(statement, columnIndex++, field.schema(), ((Struct) record.key()).get(field));
        }
        return columnIndex;
    }

    private int bindValueFields(SinkRecord record, Struct valueStruct, int columnIndex) throws SQLException {
        for (String fieldName : kafkaFieldsMetadata.getValueFields()) {
            Field field = record.valueSchema().field(fieldName);
            bindField(statement, columnIndex++, field.schema(), valueStruct.get(field));
        }
        return columnIndex;
    }

}