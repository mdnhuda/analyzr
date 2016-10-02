package org.analyzr.service;

import com.google.common.collect.Lists;
import org.analyzr.domain.DbTable;
import org.analyzr.domain.DbTableColumn;
import org.analyzr.utils.DataTypeUtils;
import org.analyzr.utils.DateUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
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
        jdbcTemplate.update(table.getInsertQuery(), table.getInsertParams(params));
    }

    public DbTable populateTable(File csvFile) {
        try {
            CSVParser csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());

            Map<String, Integer> headerPositionMap = csvParser.getHeaderMap();
            Map<String, DbTableColumn.Type> columnTypeMap = new LinkedHashMap<>();
            Map<String, Integer> columnLengthMap = new LinkedHashMap<>();
            for (CSVRecord record : csvParser.getRecords()) {
                for (Map.Entry<String, Integer> headerEntry: headerPositionMap.entrySet()) {
                    String val = record.get(headerEntry.getValue());
                    DbTableColumn.Type existingType = columnTypeMap.get(headerEntry.getKey());
                    DbTableColumn.Type newType = DataTypeUtils.determineType(val);

                    if (existingType == null && newType != null) {
                        columnTypeMap.put(headerEntry.getKey(), newType);
                        columnLengthMap.put(headerEntry.getKey(), val.length());
                    } else if (existingType != null && newType != null) {
                        if (newType.ordinal() > existingType.ordinal()) {
                            columnTypeMap.put(headerEntry.getKey(), newType);
                        }
                        int currentLength = columnLengthMap.getOrDefault(headerEntry.getKey(), 50);
                        if (val.length() > currentLength) {
                            columnLengthMap.put(headerEntry.getKey(), val.length());
                        }
                    }
                }
            }

            DbTable table = new DbTable(FilenameUtils.getBaseName(csvFile.getName()));
            for (Map.Entry<String, Integer> entry : csvParser.getHeaderMap().entrySet()) {
                table.addColumn(new DbTableColumn(entry.getKey(),
                        columnTypeMap.getOrDefault(entry.getKey(), DbTableColumn.Type.VARCHAR))
                        .withLength(columnLengthMap.getOrDefault(entry.getKey(), 50)));
            }

            create(table);
            csvParser = CSVParser.parse(csvFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());
            for (CSVRecord record : csvParser.getRecords()) {
                List<String> paramList = Lists.newArrayList(record.iterator());
                insert(table, paramList.toArray(new String[paramList.size()]));
            }

            return table;

        } catch (IOException e) {
            throw new RuntimeException("Could not parse csv file", e);
        }
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
