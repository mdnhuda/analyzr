package org.analyzr.domain;

import com.google.common.base.Preconditions;
import org.analyzr.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author nhuda
 * @since 29/09/16
 */
public class DbTableColumn {
    private Logger log = LoggerFactory.getLogger(getClass());

    public enum Type {
        BOOLEAN,
        INTEGER,
        DATE,
        VARCHAR;

        public boolean hasLength() {
            return this == VARCHAR;
        }

    }

    public DbTableColumn(String name, Type type) {
        this.name = name;
        this.type = type;
        if (type == Type.VARCHAR) {
            length = 50;
        }
    }

    private String name;
    private Type type = Type.VARCHAR;
    private int length = 50;
    private boolean nullable = true;
    private boolean unique = false;
    private DbTable table;

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public int getLength() {
        return length;
    }

    public boolean isNullable() {
        return nullable;
    }

    public boolean isUnique() {
        return unique;
    }

    public DbTable getTable() {
        return table;
    }

    public void setTable(DbTable table) {
        this.table = table;
    }

    public String getQueryAlias() {
        return table.getAlias() + "_" + name;
    }
    public DbTableColumn withLength(int length) {
        Preconditions.checkArgument(length > 0, "Valid Length required");
        this.length = length;
        return this;
    }

    public DbTableColumn withUnique(boolean unique) {
        this.unique = unique;
        return this;
    }

    public DbTableColumn withNullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public String createTableColumn() {
        Preconditions.checkArgument(!StringUtils.isEmpty(name), "Name can't be null");
        Preconditions.checkArgument(!StringUtils.isEmpty(type), "Type can't be null");
        Preconditions.checkArgument(!(type.hasLength() && length <= 0), "Length is required for VARCHAR");

        StringBuilder column = new StringBuilder(name).append(" ").append(type.name());

        if (type.hasLength()) {
            column.append("(").append(length).append(")");
        }

        if (!nullable) {
            column.append(" NOT NULL");
        }

        if (unique) {
            column.append(" UNIQUE");
        }

        return column.toString();
    }

    public Object ingest(String raw) {
        Preconditions.checkArgument(!(raw == null && !nullable), "Column is not nullable, but the argument is null");
        if (raw == null) {
            return null;
        }

        Object val = null;
        switch (type) {
            case DATE:
                try {
                    val = DateUtils.parseDate(raw);
                } catch (ParseException e) {
                    log.error("Could not parse the value={} to a date", raw);
                }
                break;

            case INTEGER:
                try {
                    val = Integer.parseInt(raw);
                } catch (NumberFormatException e) {
                    log.error("Could not parse the value={} to an integer", raw);
                }
                break;

            case BOOLEAN:
                val = (raw.equalsIgnoreCase("yes") || raw.equalsIgnoreCase("true") || raw.equalsIgnoreCase("1"));
                break;

            default:
                val = raw;
        }
        return val;
    }
}
