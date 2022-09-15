package lawrencemq.SnowflakeSinkConnector.sql;


import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;

public class TableDescription {
    private final Table table;
    private final LinkedHashMap<String, ColumnDescription> columnsByName;

    public TableDescription(Table table, LinkedHashMap<String, ColumnDescription> columnsByName) {
        this.table = table;
        this.columnsByName = columnsByName;
    }

    public Table getTable() {
        return table;
    }

    public Collection<ColumnDescription> descriptionsForColumns() {
        return columnsByName.values();
    }


    public Set<String> columnNames() {
        return columnsByName.keySet();
    }

    @Override
    public int hashCode() {
        return table.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableDescription that = (TableDescription) o;
        return table.equals(that.table) && columnsByName.equals(that.columnsByName);
    }

    @Override
    public String toString() {
        return String.format("Table{name=%s,columns=%s}", table, descriptionsForColumns()
        );
    }
}