package lawrencemq.SnowflakeSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

// todo make this my own
public class TableDoesNotExistException extends ConnectException {
    public TableDoesNotExistException(String message) {
        super(message);
    }
    public TableDoesNotExistException(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
