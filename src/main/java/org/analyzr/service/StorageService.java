package org.analyzr.service;

import org.analyzr.domain.DbTable;
import org.analyzr.domain.DbTableColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author nhuda
 * @since 29/09/16
 */
@Service
public class StorageService {

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public StorageService(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    public void create(DbTable table) {
        jdbcTemplate.execute(table.getCreateTableQuery());
    }

    public void insert(DbTable table, String ... params) {
        jdbcTemplate.update(table.getInsertQuery(params), table.getInsertParams(params));
    }

    public void drop(DbTable table) {
        jdbcTemplate.execute(table.getDropTableQuery());
    }

    public void delete(DbTable table) {
        jdbcTemplate.execute(table.getCleanTableQuery());
    }

    public List<Map<String, Object>> retrieve(DbTable table) {
        return jdbcTemplate.query(table.getRetrieveQuery(), new ResultSetExtractor<List<Map<String, Object>>>() {
            @Override
            public List<Map<String, Object>> extractData(ResultSet rs) throws SQLException, DataAccessException {
                List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<String, Object>();
                    for (DbTableColumn column: table.getColumns()) {
                        row.put(column.getName(), rs.getObject(column.getQueryAlias()));
                    }
                    result.add(row);
                }
                return result;
            }
        });
    }
}
