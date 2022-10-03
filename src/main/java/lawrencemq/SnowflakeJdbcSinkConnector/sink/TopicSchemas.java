package lawrencemq.SnowflakeJdbcSinkConnector.sink;
import org.apache.kafka.connect.data.Schema;

import java.util.Objects;

public final class TopicSchemas {
    private final Schema keySchema;
    private final Schema valueSchema;

    public TopicSchemas(Schema keySchema, Schema valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    public Schema keySchema() {
        return keySchema;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (TopicSchemas) obj;
        return Objects.equals(this.keySchema, that.keySchema) &&
                Objects.equals(this.valueSchema, that.valueSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySchema, valueSchema);
    }

    @Override
    public String toString() {
        return String.format("TopicSchemas{keySchema=%s,valueSchema=%s}", this.keySchema, this.valueSchema);
    }
}
