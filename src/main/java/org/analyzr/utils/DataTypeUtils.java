package org.analyzr.utils;

import org.analyzr.domain.DbTableColumn;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Created by naimulhuda on 2/10/2016.
 */
public class DataTypeUtils {
    private static final List<String> BOOLEAN_VARIANTS = Arrays.asList("0", "1", "true", "false", "yes", "no");

    public static DbTableColumn.Type determineType(String val) {
        if (StringUtils.isEmpty(val)) {
            return null;
        } else if (BOOLEAN_VARIANTS.contains(val)) {
            return DbTableColumn.Type.BOOLEAN;
        } else if (val.matches("\\d+")) {
            return DbTableColumn.Type.INTEGER;
        } else if (DateUtils.determineDateFormat(val) != null) {
            return DbTableColumn.Type.DATE;
        } else {
            return DbTableColumn.Type.VARCHAR;
        }
    }

}
