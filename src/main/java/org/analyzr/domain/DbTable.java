package org.analyzr.domain;

import com.google.common.base.Preconditions;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author nhuda
 * @since 29/09/16
 */
public class DbTable {
    private String tableName;
    private String alias;
    private List<DbTableColumn> columns;

    public DbTable(String tableName) {
        this.tableName = tableName;
        this.alias = tableName;
        columns = new ArrayList<>();
    }

    public DbTable(String tableName, String alias) {
        this.tableName = tableName;
        this.alias = alias;
        columns = new ArrayList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    public List<DbTableColumn> getColumns() {
        return columns;
    }

    public void addColumn(DbTableColumn column) {
        if (columns == null) {
            columns = new ArrayList<>();
        }
        column.setTable(this);
        columns.add(column);
    }

    public String getCreateTableQuery() {
        Preconditions.checkArgument(!StringUtils.isEmpty(tableName), "Table must have a name!");
        Preconditions.checkArgument(columns != null && columns.size() > 0, "Table must have at least one column!");
        return
                "CREATE TABLE " + tableName
                        + " ("
                        + columns.stream().map(DbTableColumn::createTableColumn).collect(Collectors.joining(", "))
                        + ")";
    }

    public String getDropTableQuery() {
        return "DROP TABLE IF EXISTS " + tableName + " CASCADE";
    }

    public String getCleanTableQuery() {
        return "DELETE FROM " + tableName;
    }

    public String getRetrieveQuery() {
        return "SELECT "
                + columns.stream()
                .map(col -> alias + "." + col.getName() + " AS " + col.getQueryAlias())
                .collect(Collectors.joining(", "))
                + " FROM " + tableName + " AS " + alias;
    }

    public String getCountQuery() {
        return "SELECT count(1) FROM " + tableName;
    }

    public String getInsertQuery() {
        Preconditions.checkArgument(columns != null && columns.size() > 0, "Table must have at least one column!");
        return "INSERT INTO " + tableName + " VALUES ("
                + IntStream.range(0, columns.size()).mapToObj(i -> "?").collect(Collectors.joining(", "))
                + ")";
    }
    public String getInsertQuery(String ...params) {
        Preconditions.checkArgument(columns != null && columns.size() > 0, "Table must have at least one column!");
        Preconditions.checkArgument(columns.size() == params.length,
                "Insert arguments does not match with table def, arguments len=%s, # of columns=%s",
                params.length, columns.size());

        return "INSERT INTO " + tableName + " VALUES ("
                + IntStream.range(0, params.length).mapToObj(i -> "?").collect(Collectors.joining(", "))
                + ")";
    }

    public Object[] getInsertParams(String ...rawParams) {
        Preconditions.checkArgument(columns != null && columns.size() > 0, "Table must have at least one column!");
        Preconditions.checkArgument(columns.size() == rawParams.length,
                "Insert arguments does not match with table def, arguments len={}, # of columns={}",
                rawParams.length, columns.size());
        Object[] objParams = new Object[rawParams.length];
        for (int i = 0; i < columns.size(); i++) {
            objParams[i] = columns.get(i).ingest(rawParams[i]);
        }
        return objParams;
    }

}
