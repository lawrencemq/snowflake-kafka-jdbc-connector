package lawrencemq.SnowflakeSinkConnector.sql;


import lawrencemq.SnowflakeSinkConnector.sink.KafkaColumnMetadata;
import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.stream.Collectors;


public final class SnowflakeSql {

    public static String buildInsertStatement(Table table, Collection<KafkaColumnMetadata> columns) {
        String questionMarks = columns.stream()
                .map(column -> column.getKafkaSchema().type().isPrimitive() ? "?" : "parse_json(?)")
                .collect(Collectors.joining(","));

        return QueryBuilder.newQuery().append("INSERT INTO ")
                .append(table)
                .append("(")
                .appendColumns(columns)
                .append(") SELECT ")
                .append(questionMarks)
                .append(System.lineSeparator())
                .toString();
    }

    public static String buildCreateTableStatement(Table table, Collection<Schema> fields) {
        return QueryBuilder.newQuery()
                .append("CREATE TABLE ")
                .appendTableName(table)
                .append(" (")
                .appendColumnsSpec(fields)
                .append(")")
                .toString();
    }


    public static String buildAlterTableStatement(Table table, Collection<Schema> fields) {
        boolean multipleFields = fields.size() > 1;
        QueryBuilder queryBuilder = QueryBuilder.newQuery();

        QueryBuilder.Transform<Schema> transform = (field) -> {
            if (multipleFields) {
                queryBuilder.appendNewLine();
            }
            queryBuilder.append("ADD ")
                    .appendColumnSpec(field);
        };

        return queryBuilder
                .append("ALTER TABLE ")
                .append(table)
                .append(" ")
                .appendList()
                .withTransformation(transform)
                .withItems(fields)
                .toString();
    }


}
