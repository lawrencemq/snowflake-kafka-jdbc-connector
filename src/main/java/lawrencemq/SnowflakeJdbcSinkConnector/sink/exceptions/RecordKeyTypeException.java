package lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

public class RecordKeyTypeException extends ConnectException {
    public RecordKeyTypeException(String message) {
        super(message);
    }
}
