package org.analyzr.service;

import org.analyzr.config.DaoConfig;
import org.analyzr.config.RootConfig;
import org.analyzr.config.ServletConfig;
import org.analyzr.domain.DbTable;
import org.analyzr.domain.DbTableColumn;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;

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

    @Before
    public void setUp() throws Exception {
        jdbcTemplate = new JdbcTemplate(dataSource);
        table = new DbTable("product", "p");
        table.addColumn(new DbTableColumn("id", DbTableColumn.Type.INTEGER).withUnique(true).withNullable(false));
        table.addColumn(new DbTableColumn("code", DbTableColumn.Type.VARCHAR));
    }

    @Test
    public void testInsert() throws Exception {
//        storageService.drop(table);

        storageService.create(table);

        storageService.insert(table, "1", "ABC");

        assertEquals(Long.valueOf(1L),
                jdbcTemplate.queryForObject("SELECT count(1) FROM product", new Object[]{}, Long.class));
    }

}