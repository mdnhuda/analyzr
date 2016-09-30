package org.analyzr.domain;

import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author nhuda
 * @since 30/09/16
 */
public class DbTableColumnTest {

    @Test
    public void testCreateTableColumn() throws Exception {
        DbTableColumn column = new DbTableColumn("first_name", DbTableColumn.Type.VARCHAR);
        assertEquals("Create Column with default", "first_name VARCHAR(50)", column.createTableColumn());

        column = new DbTableColumn("id", DbTableColumn.Type.INTEGER).withNullable(false).withUnique(true);
        assertEquals("Create Integer Column with length", "id INTEGER NOT NULL UNIQUE", column.createTableColumn());

        column = new DbTableColumn("name", DbTableColumn.Type.VARCHAR).withLength(1000);
        assertEquals("Create Varchar Column with length", "name VARCHAR(1000)", column.createTableColumn());

        column = new DbTableColumn("created_date", DbTableColumn.Type.DATE);
        assertEquals("Create Date Column", "created_date DATE", column.createTableColumn());

        column = new DbTableColumn("created_date", DbTableColumn.Type.DATE).withLength(10);
        assertEquals("Create Date Column with Length ignored", "created_date DATE", column.createTableColumn());

        column = new DbTableColumn("is_new", DbTableColumn.Type.BOOLEAN);
        assertEquals("Create Boolean Column", "is_new BOOLEAN", column.createTableColumn());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testLengthToSmallForVarchar() {
        DbTableColumn column = new DbTableColumn("first_name", DbTableColumn.Type.VARCHAR).withLength(-1);
        column.createTableColumn();
    }

    @Test
    public void testColumnValueIngestion() {
        DbTableColumn column = new DbTableColumn("id", DbTableColumn.Type.INTEGER).withLength(10);
        assertEquals("Should match parsed integer value", 999, column.ingest("999"));
        assertNull("Should match parsed Integer value from 'null'", column.withNullable(true).ingest(null));

        column = new DbTableColumn("is_new", DbTableColumn.Type.BOOLEAN);
        assertEquals("Should match parsed Boolean value from 'Yes'", true, column.ingest("Yes"));
        assertEquals("Should match parsed Boolean value from 'True'", true, column.ingest("True"));
        assertEquals("Should match parsed Boolean value from '1'", true, column.ingest("1"));
        assertEquals("Should match parsed Boolean value from 'xx'", false, column.ingest("xx"));
        assertEquals("Should match parsed Boolean value from '0'", false, column.ingest("0"));
        assertNull("Should match parsed Boolean value from 'null'", column.withNullable(true).ingest(null));

        column = new DbTableColumn("created_date", DbTableColumn.Type.DATE);
        assertEquals("Should match parsed Date value for 'yyyy-mm-dd'",
                new DateTime(2016, 1, 31, 0, 0).toDate(), column.ingest("2016-01-31"));
        assertEquals("Should match parsed Date value for 'yyyy/mm/dd'",
                new DateTime(2016, 1, 31, 0, 0).toDate(), column.ingest("2016/01/31"));
        assertEquals("Should match parsed Date value for 'dd-mm-yyyy'",
                new DateTime(2016, 12, 13, 0, 0).toDate(), column.ingest("13-12-2016"));
        assertEquals("Should match parsed Date value for 'yyyy-mm-dd HH:mm'",
                new DateTime(2016, 12, 13, 14, 30).toDate(), column.ingest("2016-12-13 14:30"));
        assertNull("Should match parsed Date value from 'null'", column.withNullable(true).ingest(null));

        column = new DbTableColumn("name", DbTableColumn.Type.VARCHAR);
        assertEquals("Should match String value", "abcd", column.ingest("abcd"));
        assertEquals("Should match null String value", null, column.ingest(null));
    }
}