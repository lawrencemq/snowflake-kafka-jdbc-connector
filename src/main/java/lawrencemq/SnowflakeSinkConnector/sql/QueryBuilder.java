package lawrencemq.SnowflakeSinkConnector.sql;


import org.apache.kafka.connect.data.Schema;

import java.util.*;

import static java.util.Objects.nonNull;
import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.formatColumnValue;
import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.getSqlTypeFromAvro;
import static lawrencemq.SnowflakeSinkConnector.sql.QueryUtils.bytesToHex;

final class QueryBuilder {
    private final StringBuilder sb = new StringBuilder();

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

        if (Objects.nonNull(f.defaultValue())) {
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

    QueryBuilder appendColumns(LinkedHashMap<String, Schema> fieldsToSchemaMap) {
        Transform<String> transform = (field) -> {
            append(System.lineSeparator()).appendColumnName(field);
        };

        appendList()
                .withTransformation(transform)
                .withItems(fieldsToSchemaMap.keySet());
        return this;
    }

    QueryBuilder appendColumnName(String columnName) {
        append(convertColumnName(columnName));
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