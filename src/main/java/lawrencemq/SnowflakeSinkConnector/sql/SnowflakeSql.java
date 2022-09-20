package lawrencemq.SnowflakeSinkConnector.sql;


import org.apache.kafka.connect.data.Schema;

import java.util.*;
import java.util.stream.Collectors;


public final class SnowflakeSql {

    public static String buildInsertStatement(Table table, LinkedHashMap<String, Schema> kafkaFieldToSchemaMap) {
        String questionMarks = kafkaFieldToSchemaMap.values().stream()
                .map(schema -> schema.type().isPrimitive() ? "?" : "parse_json(?)")
                .collect(Collectors.joining(","));

        return QueryBuilder.newQuery().append("INSERT INTO ")
                .append(table)
                .append("(")
                .appendColumns(kafkaFieldToSchemaMap)
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
