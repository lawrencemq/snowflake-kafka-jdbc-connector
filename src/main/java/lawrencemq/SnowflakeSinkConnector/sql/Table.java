package lawrencemq.SnowflakeSinkConnector.sql;

public final class Table {
    private final String databaseName;
    private final String schemaName;
    private final String tableName;

    public Table(String databaseName, String schemaName, String tableName) {
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Table table = (Table) o;

        if (!databaseName.equals(table.databaseName)) return false;
        if (!schemaName.equals(table.schemaName)) return false;
        return tableName.equals(table.tableName);
    }

    @Override
    public int hashCode() {
        int result = databaseName.hashCode();
        result = 31 * result + schemaName.hashCode();
        result = 31 * result + tableName.hashCode();
        return result;
    }

    public String toString() {
        return String.format("\"%s\".\"%s\".\"%s\"", databaseName, schemaName, tableName);
    }


}
