package org.analyzr.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author naimulhuda
 * @since 1/10/2016
 */
public class JoinTable {
    private String tableName;
    private String alias;
    private List<DbTableColumn> columns;
    private DbTable primary;
    private DbTable secondary;
    private JoinColumnPair joinColumnPair;

    public JoinTable(DbTable primary, DbTable secondary, String joinOnColumn) {
        this(primary, secondary, joinOnColumn, joinOnColumn);
    }

    public JoinTable(DbTable primary, DbTable secondary, String leftColumnName, String rightColumnName) {
        this.tableName = primary.getTableName() + "_" + secondary.getTableName();
        this.alias = tableName;

        this.primary = primary;
        this.secondary = secondary;
        DbTableColumn left = primary.getColumns().stream().filter(col -> col.getName().equals(leftColumnName)).findFirst().get();
        DbTableColumn right = secondary.getColumns().stream().filter(col -> col.getName().equals(rightColumnName)).findFirst().get();
        this.joinColumnPair = new JoinColumnPair(left, right);
        columns = new ArrayList<>();
        columns.addAll(primary.getColumns());
        secondary.getColumns().forEach(col -> {
            if (col != right) {
                columns.add(col);
            }
        });
    }

    public String getRetrieveQuery() {
        return "SELECT "
                + columns.stream()
                .map(col -> col.getTable().getAlias() + "." + col.getName() + " AS " + col.getQueryAlias())
                .collect(Collectors.joining(", "))
                + " FROM " + primary.getTableName() + " AS " + primary.getAlias()
                + " JOIN " + secondary.getTableName() + " AS " + secondary.getAlias()
                + " ON " + joinColumnPair.getJoinON();

    }

    private static class JoinColumnPair {
        private DbTableColumn left;
        private DbTableColumn right;

        public JoinColumnPair(DbTableColumn left, DbTableColumn right) {
            this.left = left;
            this.right = right;
        }

        public DbTableColumn getLeft() {
            return left;
        }

        public void setLeft(DbTableColumn left) {
            this.left = left;
        }

        public DbTableColumn getRight() {
            return right;
        }

        public void setRight(DbTableColumn right) {
            this.right = right;
        }
        public String getJoinON() {
            return left.getTable().getAlias() + "." + left.getName()
                    + " = " + right.getTable().getAlias() + "." + right.getName();
        }
    }
}
