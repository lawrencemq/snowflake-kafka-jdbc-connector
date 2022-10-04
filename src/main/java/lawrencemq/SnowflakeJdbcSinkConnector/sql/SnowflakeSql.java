package lawrencemq.SnowflakeJdbcSinkConnector.sql;


import org.apache.kafka.connect.data.*;

import java.util.*;
import java.util.stream.Collectors;


public final class SnowflakeSql {

    /**
     * Note: for speedup/boost, if all fields are primitives, the insert will use VALUES instead of SELECt in order
     * to use Snowflake's temporary staging and resulting in larger batch inserts instead of multiple individual inserts.
     * @param table
     * @param kafkaFieldToSchemaMap
     * @return
     */
    public static String buildInsertStatement(Table table, LinkedHashMap<String, Field> kafkaFieldToSchemaMap) {
        String questionMarks = kafkaFieldToSchemaMap.values().stream()
                .map(field -> field.schema().type().isPrimitive() ? "?" : "parse_json(?)")
                .collect(Collectors.joining(","));

        boolean allFieldsArePrimitive = kafkaFieldToSchemaMap.values().stream()
                .allMatch(field -> field.schema().type().isPrimitive());

        return QueryBuilder.newQuery().append("INSERT INTO ")
                .append(table)
                .append("(")
                .appendColumns(kafkaFieldToSchemaMap)
                .append(") ")
                .append(allFieldsArePrimitive ? "VALUES (" : "SELECT ")
                .append(questionMarks)
                .append(allFieldsArePrimitive ? ")" : "")
                .append(System.lineSeparator())
                .toString();
    }

    public static String buildCreateTableStatement(Table table, Collection<Field> fields) {
        return QueryBuilder.newQuery()
                .append("CREATE TABLE ")
                .appendTableName(table)
                .append(" (")
                .appendColumnsSpec(fields)
                .append(")")
                .toString();
    }


    public static String buildAlterTableStatement(Table table, Collection<Field> fields) {
        boolean multipleFields = fields.size() > 1;
        QueryBuilder queryBuilder = QueryBuilder.newQuery();

        QueryBuilder.Transform<Field> transform = (field) -> {
            if (multipleFields) {
                queryBuilder.appendNewLine();
            }
            queryBuilder.appendColumnSpec(field);
        };

        return queryBuilder
                .append("ALTER TABLE ")
                .append(table)
                .append(" ADD ")
                .appendList()
                .withTransformation(transform)
                .withItems(fields)
                .toString();
    }


}
