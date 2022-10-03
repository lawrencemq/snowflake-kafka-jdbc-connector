package lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

public class TableAlterOrCreateException extends ConnectException {
    public TableAlterOrCreateException(String message) {
        super(message);
    }
    public TableAlterOrCreateException(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
