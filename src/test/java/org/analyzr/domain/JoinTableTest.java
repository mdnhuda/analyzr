package org.analyzr.domain;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author naimulhuda
 * @since 1/10/2016
 */
public class JoinTableTest {

    @Test
    public void testGetRetrieveQuery() throws Exception {
        DbTable productTable = new DbTable("product", "prd");
        productTable.addColumn(new DbTableColumn("id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        productTable.addColumn(new DbTableColumn("category", DbTableColumn.Type.VARCHAR));
        productTable.addColumn(new DbTableColumn("name", DbTableColumn.Type.VARCHAR).withLength(1000));

        DbTable priceTable = new DbTable("price", "prc");
        priceTable.addColumn(new DbTableColumn("product_id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        priceTable.addColumn(new DbTableColumn("price", DbTableColumn.Type.INTEGER));

        JoinTable productPrice = new JoinTable(productTable, priceTable, "id", "product_id");
        assertEquals("Join Query should match",
                "SELECT prd.id AS prd_id, prd.category AS prd_category, prd.name AS prd_name, prc.price AS prc_price"
                        + " FROM product AS prd"
                        + " JOIN price AS prc ON prd.id = prc.product_id",
                productPrice.getJoinedQuery()
        );
    }
}