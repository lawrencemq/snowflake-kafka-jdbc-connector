package lawrencemq.SnowflakeSinkConnector.sink;


import lawrencemq.SnowflakeSinkConnector.sink.exceptions.RecordValidationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Objects;

final class RecordValidator {

    static void validate(SinkRecord record) {
        validateKey(record);
        validateValue(record);
    }

    private static void validateKey(SinkRecord record) {
        Schema keySchema = record.keySchema();
        Object key = record.key();
        if (Objects.isNull(key) && Objects.isNull(keySchema)) {
            return;
        }
        if (Objects.nonNull(key) && Objects.nonNull(keySchema) && keySchema.type() == Schema.Type.STRUCT) {
            return;
        }
        throw new RecordValidationException(
                String.format(
                        "Snowflake sink connector requires " +
                                "records to have null keys or non-null Avro Struct keys and schemas. " +
                                "Found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) " +
                                "with a %s value and %s value schema.",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset(),
                        record.timestamp(),
                        getValueType(key),
                        getSchemaType(record.keySchema())
                )
        );
    }

    private static void validateValue(SinkRecord record) {
        Schema valueSchema = record.valueSchema();
        Object value = record.value();
        if (Objects.nonNull(value) && Objects.nonNull(valueSchema) && valueSchema.type() == Schema.Type.STRUCT) {
            return;
        }
        throw new RecordValidationException(
                String.format(
                        "Snowflake sink connector requires " +
                                "records to have non-null Avro Struct values and schemas. " +
                                "Found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) " +
                                "with a %s value and %s value schema.",
                        record.topic(),
                        record.kafkaPartition(),
                        record.kafkaOffset(),
                        record.timestamp(),
                        getValueType(value),
                        getSchemaType(record.valueSchema())
                )
        );
    }

    private static String getValueType(Object value) {
        return Objects.isNull(value) ? "NULL" : value.getClass().getSimpleName();
    }

    private static String getSchemaType(Schema schema) {
        if (Objects.isNull(schema)) {
            return "NULL";
        } else if (schema.type() == Schema.Type.STRUCT) {
            return "Struct";
        }
        return schema.type().getName();
    }

}