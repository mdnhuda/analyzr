package org.analyzr.domain;

import org.analyzr.config.DaoConfig;
import org.analyzr.config.RootConfig;
import org.analyzr.config.ServletConfig;
import org.analyzr.domain.DbTable;
import org.analyzr.domain.DbTableColumn;
import org.analyzr.service.StorageService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author nhuda
 * @since 30/09/16
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration(classes = {
        RootConfig.class,
        DaoConfig.class})
public class StorageServiceTest {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private StorageService storageService;

    private JdbcTemplate jdbcTemplate;
    private DbTable table;
    private DbTable secondTable;

    @Before
    public void setUp() throws Exception {
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = new DbTable("product", "p");
        table.addColumn(new DbTableColumn("id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        table.addColumn(new DbTableColumn("code", DbTableColumn.Type.VARCHAR));

        secondTable = new DbTable("price", "prc");
        secondTable.addColumn(new DbTableColumn("product_id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        secondTable.addColumn(new DbTableColumn("price", DbTableColumn.Type.INTEGER));

        storageService.create(table);
        storageService.create(secondTable);

        storageService.insert(table, "1", "ABC");
        storageService.insert(table, "2", "XYZ");
        storageService.insert(table, "3", "PQR");

        storageService.insert(secondTable, "2", "99");
        storageService.insert(secondTable, "1", "101");

    }

    @After
    public void clean() {
        storageService.drop(table);
        storageService.drop(secondTable);
    }

    @Test
    public void testTableOperations() throws Exception {

        assertEquals(Long.valueOf(2L),
                jdbcTemplate.queryForObject("SELECT count(1) FROM product", new Object[]{}, Long.class));

        storageService.delete(table);

        assertEquals(Long.valueOf(0L),
                jdbcTemplate.queryForObject("SELECT count(1) FROM product", new Object[]{}, Long.class));
    }

    @Test
    public void testJoinTableQuery() throws Exception {

        Map<String, Integer> res = jdbcTemplate.query(
                new JoinTable(table, secondTable, "id", "product_id").getRetrieveQuery(),
                new Object[]{}, rs -> {
                    Map<String, Integer> res1 = new HashMap<>();
                    while (rs.next()) {
                        res1.put(rs.getString("p_code"), rs.getInt("prc_price"));
                    }
                    return res1;
                });
        assertEquals("Should map product ABC to 101", Integer.valueOf(101), res.get("ABC"));
        assertEquals("Should map product XYZ to 99", Integer.valueOf(99), res.get("XYZ"));
        assertFalse("Should not contain product PQR", res.containsKey("PQR"));
    }
}
