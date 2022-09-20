package lawrencemq.SnowflakeSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

public class RecordValidationException extends ConnectException {
    public RecordValidationException(String message) {
        super(message);
    }
}
