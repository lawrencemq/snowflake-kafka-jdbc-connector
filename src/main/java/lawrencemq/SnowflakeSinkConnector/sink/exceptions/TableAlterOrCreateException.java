package lawrencemq.SnowflakeSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

// todo make this my own
public class TableAlterOrCreateException extends ConnectException {
    public TableAlterOrCreateException(String message) {
        super(message);
    }
    public TableAlterOrCreateException(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
