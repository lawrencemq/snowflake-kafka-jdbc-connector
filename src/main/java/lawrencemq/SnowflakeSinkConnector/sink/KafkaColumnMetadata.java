package lawrencemq.SnowflakeSinkConnector.sink;

import org.apache.kafka.connect.data.Schema;

import java.util.Objects;

public final class KafkaColumnMetadata {
    private final String columnName;
    private final Schema kafkaSchema;

    KafkaColumnMetadata(String columnName, Schema kafkaSchema) {
        this.columnName = columnName;
        this.kafkaSchema = kafkaSchema;
    }

    public String getColumnName() {
        return columnName;
    }

    public Schema getKafkaSchema() {
        return kafkaSchema;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (KafkaColumnMetadata) obj;
        return Objects.equals(this.columnName, that.columnName) &&
                Objects.equals(this.kafkaSchema, that.kafkaSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, kafkaSchema);
    }

    @Override
    public String toString() {
        return String.format("KafkaColumnMetadata{columnName=%s,schema=%s}", columnName, kafkaSchema);
    }

}
