package org.analyzr.domain;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author nhuda
 * @since 30/09/16
 */
public class DbTableTest {

    @Test
    public void testGetCreateTableQuery() throws Exception {
        DbTable table = new DbTable("product");
        table.addColumn(new DbTableColumn("id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        table.addColumn(new DbTableColumn("category", DbTableColumn.Type.VARCHAR));
        table.addColumn(new DbTableColumn("name", DbTableColumn.Type.VARCHAR).withLength(1000));
        assertEquals("Create table query should match",
                "CREATE TABLE product (" +
                        "id INTEGER NOT NULL UNIQUE" +
                        ", category VARCHAR(50)" +
                        ", name VARCHAR(1000)" +
                        ")",
                table.getCreateTableQuery());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetCreateTableQueryError() throws Exception {
        DbTable table = new DbTable("product");
        table.getCreateTableQuery();
    }

    @Test
    public void testDropTableQuery() throws Exception {
        DbTable table = new DbTable("product");
        assertEquals("Should match", "DROP TABLE product", table.getDropTableQuery());
    }

    @Test
    public void testCleanTableQuery() throws Exception {
        DbTable table = new DbTable("product");
        assertEquals("Should match", "DELETE FROM product", table.getCleanTableQuery());
    }

    @Test
    public void testInsertQuery() throws Exception {
        DbTable table = new DbTable("product");
        table.addColumn(new DbTableColumn("id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        table.addColumn(new DbTableColumn("category", DbTableColumn.Type.VARCHAR));
        table.addColumn(new DbTableColumn("name", DbTableColumn.Type.VARCHAR).withLength(1000));
        table.addColumn(new DbTableColumn("create_date", DbTableColumn.Type.DATE));
        table.addColumn(new DbTableColumn("in_stock", DbTableColumn.Type.BOOLEAN));

        DateTime dateTime = new DateTime(2016, 9, 30, 0, 0);
        String[] rawInsertValues = new String[] {"1001", "Food", "Nachos", "2016-09-30", "Yes"};
        assertEquals("Should match insert query",
                "INSERT INTO product VALUES (?, ?, ?, ?, ?)",
                table.getInsertQuery(rawInsertValues));

        assertArrayEquals("Should match insert params",
                new Object[] {1001, "Food", "Nachos", dateTime.toDate(), true},
                table.getInsertParams(rawInsertValues));
    }

    @Test
    public void testRetrieveQuery() throws Exception {
        DbTable table = new DbTable("product", "p");
        table.addColumn(new DbTableColumn("id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        table.addColumn(new DbTableColumn("code", DbTableColumn.Type.VARCHAR));

        assertEquals("Select Query Should match",
                "SELECT p.id AS p_id, p.code AS p_code FROM product AS p",
                table.getRetrieveQuery());
    }
}