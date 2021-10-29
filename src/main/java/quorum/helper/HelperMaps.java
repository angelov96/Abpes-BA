package quorum.helper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class HelperMaps {

    public static Map<String, AtomicLong> getNonceMap() {
        return NONCE_MAP;
    }

    private static final Map<String, AtomicLong> NONCE_MAP = new ConcurrentHashMap<>();

    public static Map<String, Boolean> getUnlockedMap() {
        return UNLOCKED_MAP;
    }

    private static final Map<String, Boolean> UNLOCKED_MAP = new ConcurrentHashMap<>();

}
