package lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

public class RecordValueTypeException extends ConnectException {
    public RecordValueTypeException(String message) {
        super(message);
    }
}
