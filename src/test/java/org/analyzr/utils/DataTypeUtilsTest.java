package org.analyzr.utils;

import org.analyzr.domain.DbTableColumn;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by naimulhuda on 2/10/2016.
 */
public class DataTypeUtilsTest {

    @Test
    public void testDetermineBooleanType() throws Exception {
        assertEquals(DbTableColumn.Type.BOOLEAN, DataTypeUtils.determineType("true"));
        assertEquals(DbTableColumn.Type.BOOLEAN, DataTypeUtils.determineType("false"));
        assertEquals(DbTableColumn.Type.BOOLEAN, DataTypeUtils.determineType("1"));
        assertEquals(DbTableColumn.Type.BOOLEAN, DataTypeUtils.determineType("0"));
        assertEquals(DbTableColumn.Type.BOOLEAN, DataTypeUtils.determineType("yes"));
        assertEquals(DbTableColumn.Type.BOOLEAN, DataTypeUtils.determineType("no"));
    }

    @Test
    public void testDetermineNumberType() throws Exception {
        assertEquals(DbTableColumn.Type.INTEGER, DataTypeUtils.determineType("2"));
        assertEquals(DbTableColumn.Type.INTEGER, DataTypeUtils.determineType("100"));
        assertEquals(DbTableColumn.Type.INTEGER, DataTypeUtils.determineType("989899981"));
    }

    @Test
    public void testDetermineDateType() throws Exception {
        assertEquals(DbTableColumn.Type.DATE, DataTypeUtils.determineType("2015/12/31"));
        assertEquals(DbTableColumn.Type.DATE, DataTypeUtils.determineType("2015/02/01 19:35:57"));
    }

    @Test
    public void testDetermineStringType() throws Exception {
        assertEquals(DbTableColumn.Type.VARCHAR, DataTypeUtils.determineType("2a"));
        assertEquals(DbTableColumn.Type.VARCHAR, DataTypeUtils.determineType("2015/12/130"));
    }
}
