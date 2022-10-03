package lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

public class TableDoesNotExistException extends ConnectException {
    public TableDoesNotExistException(String message) {
        super(message);
    }
}
