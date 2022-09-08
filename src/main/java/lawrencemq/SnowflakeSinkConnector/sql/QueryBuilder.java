package lawrencemq.SnowflakeSinkConnector.sql;


import lawrencemq.SnowflakeSinkConnector.sink.KafkaColumnMetadata;
import org.apache.kafka.connect.data.Schema;

import javax.management.Query;
import java.util.Collection;
import java.util.Map;

import static java.util.Objects.nonNull;
import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.formatColumnValue;
import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.getSqlTypeFromAvro;
import static lawrencemq.SnowflakeSinkConnector.sql.QueryUtils.bytesToHex;

final class QueryBuilder {
    private final StringBuilder sb = new StringBuilder();

    //    private static final String DEFAULT_QUOTE = "\"";
    static QueryBuilder newQuery() {
        return new QueryBuilder();
    }

    private static String convertColumnName(String name) {
        String upperName = name.toUpperCase();
        return Character.isDigit(name.charAt(0)) ? "`" + upperName + "`" : upperName;

    }

    QueryBuilder appendColumnsSpec(Collection<Schema> fields) {
        Transform<Schema> transform = (field) -> {
            append(System.lineSeparator());
            appendColumnSpec(field);
        };
        appendList()
                .withTransformation(transform)
                .withItems(fields);
        return this;
    }

    QueryBuilder appendColumnSpec(Schema f) {
        String sqlType = getSqlTypeFromAvro(f);

        append(convertColumnName(f.name()))
                .append(" ")
                .append(sqlType);

        if (f.defaultValue() != null) {
            append(" DEFAULT ");
            formatColumnValue(
                    this,
                    f,
                    f.defaultValue()
            );
        }
        if (!f.isOptional()) {
            append(" NOT NULL");
        }
        return this;
    }

    QueryBuilder appendColumns(Collection<KafkaColumnMetadata> fields) {
        Transform<KafkaColumnMetadata> transform = (field) -> {
            append(System.lineSeparator()).appendColumn(field);
        };

        appendList()
                .withTransformation(transform)
                .withItems(fields);
        return this;
    }

    QueryBuilder appendColumn(KafkaColumnMetadata column) {
        append(convertColumnName(column.getColumnName()));
        return this;
    }


    /**
     * Transform objects while appending to a Query Builder
     */
    @FunctionalInterface
    public interface Transform<T> {
        void apply(T input);
    }


    private QueryBuilder appendStringQuote() {
        sb.append("'");
        return this;
    }

    public QueryBuilder appendStringQuoted(Object name) {
        appendStringQuote();
        sb.append(name);
        appendStringQuote();
        return this;
    }


    public QueryBuilder appendTableName(Table table) {
        sb.append(table);
        return this;
    }

    /**
     * Append a binary value as a hex string
     */
    public QueryBuilder appendBinary(byte[] value) {
        return append("x'").append(bytesToHex(value)).append("'");
    }

    public QueryBuilder appendNewLine() {
        sb.append(System.lineSeparator());
        return this;
    }

    public QueryBuilder append(Object obj) {
        if (nonNull(obj)) {
            sb.append(obj);
        }
        return this;
    }

    public <K,V> QueryBuilder appendMap(Map<K,V> map){
        if(nonNull(map)){



        }
        return this;
    }


    /**
     * Append while applying the given transformation to the object
     */
    public <T> QueryBuilder append(T obj, Transform<T> transform) {
        transform.apply(obj);
        return this;
    }


    protected final class ListBuilder<T> {
        private final static String delimiter = ",";
        private final Transform<T> transform;

        ListBuilder(Transform<T> transform) {
            this.transform = transform;
        }


        <K> ListBuilder<K> withTransformation(Transform<K> transform) {
            return new ListBuilder<>(transform);
        }

        QueryBuilder withItems(Collection<T> objects) {
            boolean needsDelimiter = false;
            for (T obj : objects) {
                if (!needsDelimiter) {
                    needsDelimiter = true;
                } else {
                    append(delimiter);
                }

                if (nonNull(transform)) {
                    append(obj, transform);
                } else {
                    append(obj);
                }
            }
            return QueryBuilder.this;
        }
    }


    ListBuilder<Object> appendList() {
        return new ListBuilder<>(null);
    }

    @Override
    public String toString() {
        return sb.toString();
    }
}