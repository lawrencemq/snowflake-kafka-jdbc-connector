package lawrencemq.SnowflakeSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

// todo make this my own
public class RecordKeyTypeException extends ConnectException {
    public RecordKeyTypeException(String message) {
        super(message);
    }
    public RecordKeyTypeException(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
