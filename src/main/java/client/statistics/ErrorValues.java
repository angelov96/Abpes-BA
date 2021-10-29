package client.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ErrorValues {

    private static final long DEFAULT_EXISTING_VALUE = -3;
    private static final long DEFAULT_ERROR_TIMESTAMP = -2;
    private static final long DEFAULT_INVALID_VALUE = -1;
    private static final List<Long> ERROR_VALUE_LIST = new ArrayList<>(
            Arrays.asList(DEFAULT_INVALID_VALUE, DEFAULT_ERROR_TIMESTAMP, DEFAULT_EXISTING_VALUE));

    public static long getDefaultExistingValue() {
        return DEFAULT_EXISTING_VALUE;
    }

    public static long getDefaultErrorTimestamp() {
        return DEFAULT_ERROR_TIMESTAMP;
    }

    public static long getDefaultInvalidValue() {
        return DEFAULT_INVALID_VALUE;
    }

    public static List<Long> getErrorValueList() {
        return ERROR_VALUE_LIST;
    }

}
