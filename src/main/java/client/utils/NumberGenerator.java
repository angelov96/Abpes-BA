package client.utils;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NumberGenerator {

    private static final Map<String, AtomicInteger> POSITION_MAP = new ConcurrentHashMap<>();

    private static final Logger LOG = Logger.getLogger(GenericSelectionStrategy.class);

    @Suspendable
    public static int selectRandomAsInt(final int min, final int max) {
        return (int) selectRandom(min, max);
    }

    @Suspendable
    public static double selectRandom(final int min, final int max) {
        return ((Math.random() * (max - min)) + min);
    }

    @Suspendable
    public static int selectRoundRobin(final String id, final int value,
                                                    final boolean reset) {

        AtomicInteger position;

        AtomicInteger atomicInteger = null;
        if (reset) {
            POSITION_MAP.computeIfPresent(id, (key, val) -> new AtomicInteger(0));
        } else {
            atomicInteger = POSITION_MAP.computeIfAbsent(id, a -> new AtomicInteger(0));
        }

        position = atomicInteger;
        LOG.debug("Current element position: " + position);

        return position.addAndGet(value);

    }

}
