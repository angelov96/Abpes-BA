package client.statistics;

import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ListenerReferenceValues {

    private ListenerReferenceValues() {
    }

    private static final Map<String, Map<String, MutablePair<Long, Long>>> TIME_MAP =
            new ConcurrentHashMap<>();

    public static Map<String, Map<String, MutablePair<Long, Long>>> getTimeMap() {
        return TIME_MAP;
    }

}
