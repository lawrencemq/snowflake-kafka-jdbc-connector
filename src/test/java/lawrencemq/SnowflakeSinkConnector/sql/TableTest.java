package lawrencemq.SnowflakeSinkConnector.sql;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {

    private final Table table = new Table("db", "schema", "table");

    @Test
    void getDatabaseName() {
        assertEquals(table.getDatabaseName(), "db");
    }

    @Test
    void getSchemaName() {
        assertEquals(table.getSchemaName(), "schema");
    }

    @Test
    void getTableName() {
        assertEquals(table.getTableName(), "table");
    }

    @Test
    void testEquals() {
        Table testTable = new Table("db", "schema", "table");
        assertEquals(table, testTable);
    }

    @Test
    void testHashCode() {
        Set<Table> tableSet = new HashSet<>();
        tableSet.add(table);
        tableSet.add(new Table("db", "schema", "table"));
        assertEquals(tableSet.size(), 1);
    }

    @Test
    void testToString() {
        assertEquals(table.toString(), "\"db\".\"schema\".\"table\"");
    }

}