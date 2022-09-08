package lawrencemq.SnowflakeSinkConnector.sql;

import lawrencemq.SnowflakeSinkConnector.sink.DateTimeUtils;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.ConnectException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static lawrencemq.SnowflakeSinkConnector.sql.ConverterUtils.convertToJSON;

final class AvroSnowflakeConverter {
    static String getSqlTypeFromAvro(Schema schema) {
        if (schema.name() != null) {
            switch (schema.name()) {
                case Decimal.LOGICAL_NAME:
                    return "NUMBER";
                case Date.LOGICAL_NAME:
                    return "DATE";
                case Time.LOGICAL_NAME:
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP_TZ"; // all timezones in UTC
                default:
                    // fall through to normal types
            }
        }

        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return "NUMBER";
            case FLOAT32:
            case FLOAT64:
                return "FLOAT";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BINARY";
            case ARRAY:
                return "ARRAY";
            case MAP:
            case STRUCT:
                return "VARIANT";
            default:
                throw new ConnectException(String.format(
                        "%s (%s) type doesn't have a mapping to the SQL database column type", schema.name(),
                        schema.type()
                ));
        }


    }

    static void formatColumnValue(QueryBuilder builder, String schemaName, Schema.Type type, Object value) {
        if (schemaName != null) {
            switch (schemaName) {
                case Decimal.LOGICAL_NAME:
                    builder.append(value);
                    return;
                case Date.LOGICAL_NAME:
                    builder.appendStringQuoted(DateTimeUtils.formatDate((java.util.Date) value));
                    return;
                case Time.LOGICAL_NAME:
                    builder.appendStringQuoted(DateTimeUtils.formatTime((java.util.Date) value));
                    return;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    builder.appendStringQuoted(
                            DateTimeUtils.formatTimestamp((java.util.Date) value)
                    );
                    return;
                default:
                    // fall through to regular types
                    break;
            }
        }
        switch (type) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case ARRAY:
            case MAP:
            case STRUCT:
                // no escaping required
                builder.append(value);
            case BOOLEAN:
                builder.append((Boolean) value ? '1' : '0');
            case STRING:
                builder.appendStringQuoted(value);
            case BYTES: {
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                builder.appendBinary(bytes);
            }
            default:
                throw new ConnectException("Unsupported type: " + type);
        }
    }


    static boolean bind(PreparedStatement statement, int index, Schema schema, Object value) throws SQLException {

        if (schema.name() != null) {
            switch (schema.name()) {
                case Date.LOGICAL_NAME:
                    statement.setDate(
                            index,
                            new java.sql.Date(((java.util.Date) value).getTime()),
                            DateTimeUtils.getTimeZoneCalendar()
                    );
                    return true;
                case Decimal.LOGICAL_NAME:
                    statement.setBigDecimal(index, (BigDecimal) value);
                    return true;
                case Time.LOGICAL_NAME:
                    statement.setTime(
                            index,
                            new java.sql.Time(((java.util.Date) value).getTime()),
                            DateTimeUtils.getTimeZoneCalendar()
                    );
                    return true;
                case org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME:
                    statement.setTimestamp(
                            index,
                            new java.sql.Timestamp(((java.util.Date) value).getTime()),
                            DateTimeUtils.getTimeZoneCalendar()
                    );
                    return true;
                default:
                    // fall through to regular types
            }
        }

        switch (schema.type()) {
            case INT8:
                statement.setByte(index, (Byte) value);
                return true;
            case INT16:
                statement.setShort(index, (Short) value);
                return true;
            case INT32:
                statement.setInt(index, (Integer) value);
                return true;
            case INT64:
                statement.setLong(index, (Long) value);
                return true;
            case FLOAT32:
                statement.setFloat(index, (Float) value);
                return true;
            case FLOAT64:
                statement.setDouble(index, (Double) value);
                return true;
            case BOOLEAN:
                statement.setBoolean(index, (Boolean) value);
                return true;
            case STRING:
                statement.setString(index, (String) value);
                return true;
            case ARRAY:
            case MAP:
            case STRUCT:
                String structJson = convertToJSON(value);
                statement.setString(index, structJson);
                return true;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                statement.setBytes(index, bytes);
                return true;
            default:
                return false;
        }
    }

}
