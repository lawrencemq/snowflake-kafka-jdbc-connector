package lawrencemq.SnowflakeSinkConnector.sql;


import lawrencemq.SnowflakeSinkConnector.sink.KafkaFieldsMetadata;
import lawrencemq.SnowflakeSinkConnector.sink.TopicSchemas;
import lawrencemq.SnowflakeSinkConnector.sink.KafkaColumnMetadata;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import java.util.stream.Collectors;


public final class SnowflakeSql {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeSql.class);
    private static final String[] TABLE_TYPES = new String[]{"TABLE"};


    public static PreparedStatement createPreparedStatement(Connection connection, String query) throws SQLException {
        log.trace("Creating a PreparedStatement '{}'", query);
        return connection.prepareStatement(query);
    }


    public static boolean tableExists(Connection connection, Table table) throws SQLException {
        log.info("Checking Snowflake - does {} exist?", table);
        try (ResultSet rs = connection.getMetaData().getTables(
                table.getDatabaseName(),
                table.getSchemaName(),
                table.getTableName(),
                TABLE_TYPES
        )) {
            boolean exists = rs.next();
            log.info("Table {} {}", table, exists ? "exists" : "does not exist");
            return exists;
        }
    }


    static LinkedHashMap<String, ColumnDescription> describeColumns(Connection connection, Table table) throws SQLException {
        log.debug("Querying for column metadata for {}", table);

        LinkedHashMap<String, ColumnDescription> results = new LinkedHashMap<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                table.getDatabaseName(),
                table.getSchemaName(),
                table.getTableName(),
                null
        )) {
            while (rs.next()) {
                final String columnName = rs.getString(4);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int nullableValue = rs.getInt(11);
                final boolean nullability = nullableValue == DatabaseMetaData.columnNullable;

                ColumnDescription description = new ColumnDescription(
                        columnName,
                        jdbcType,
                        typeName,
                        nullability
                );
                results.put(columnName, description);
            }
            return results;
        }
    }

    public static Optional<TableDescription> describeTable(Connection connection, Table table) throws SQLException {
        LinkedHashMap<String, ColumnDescription> columnDescriptions = describeColumns(connection, table);
        if (columnDescriptions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new TableDescription(table, columnDescriptions));
    }


    public static void applyDdlStatement(Connection connection, String ddlStatement) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(ddlStatement);
        }
        try {
            connection.commit();
        } catch (Exception e) { // if anything unexpected happens, rollback
            try {
                connection.rollback();
            } catch (SQLException e2) {
                e.addSuppressed(e2);
            } finally {
                throw e;
            }
        }
    }

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
        final boolean multipleFields = fields.size() > 1;
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

    public static QueryStatementBinder newStatementBinder(PreparedStatement statement, TopicSchemas topicSchemas, KafkaFieldsMetadata kafkaFieldsMetadata) {
        return new QueryStatementBinder(
                statement,
                topicSchemas,
                kafkaFieldsMetadata
        );
    }


}
