package lawrencemq.SnowflakeSinkConnector.sql;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ColumnDescriptionTest {

    @Test
    void testEquals() {
        ColumnDescription cd1 = new ColumnDescription("testName1", 1, "BOOLEAN", false);
        ColumnDescription cd2 = new ColumnDescription("testName2", 2, "BOOLEAN", true);
        Set<ColumnDescription> uniqueDescriptions = new HashSet<>();
        uniqueDescriptions.add(cd1);
        uniqueDescriptions.add(cd1);
        uniqueDescriptions.add(cd2);
        assertEquals(uniqueDescriptions.size(), 2);
        assertEquals(cd1, cd1);
        assertNotEquals(cd1, cd2);
    }

    @Test
    void testToString() {

        ColumnDescription cd = new ColumnDescription("testName", 1, "BOOLEAN", false);
        assertEquals(cd.toString(), "ColumnDescription[columnName=testName,typeName=BOOLEAN,sqlType=1,nullable=false]");
    }
}