package lawrencemq.SnowflakeSinkConnector.sql;

import lawrencemq.SnowflakeSinkConnector.sink.KafkaFieldsMetadata;
import lawrencemq.SnowflakeSinkConnector.sink.TopicSchemas;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;


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

    public void bind(SinkRecord record) throws SQLException {
        Struct valueStruct = (Struct) record.value();

        int columnIndex = 1;
        columnIndex = bindKeyFields(record, columnIndex);
        bindValueFields(record, valueStruct, columnIndex);
        statement.addBatch();
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