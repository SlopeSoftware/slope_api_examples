package com.slope.api;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
    public static String toShortDateString(Date date) {
        return new SimpleDateFormat("MM/dd/yy").format(date);
    }
}
