package client.utils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class GenericSelectionStrategy {

    private static final Map<String, AtomicInteger> POSITION_MAP = new ConcurrentHashMap<>();

    private static final Logger LOG = Logger.getLogger(GenericSelectionStrategy.class);

    public static <T> List<T> selectRandom(final List<T> selectionList,
                                                        final int numberOfElements,
                                                        final boolean withRepetitions) {

        List<List<T>> ts = (List<List<T>>) selectionList;
        T[] selectionArrayCopy = (T[]) ts.toArray();
        T[] selectionArrayTmp = (T[]) new Object[0];

        Random rand = new Random();

        if (numberOfElements > selectionArrayCopy.length && !withRepetitions) {
            throw new IndexOutOfBoundsException("More elements specified: " + numberOfElements + " than available: " + selectionArrayCopy.length);
        }

        for (int i = 0; i < numberOfElements; i++) {
            int randomIndex = rand.nextInt(selectionArrayCopy.length);
            selectionArrayTmp = ArrayUtils.add(selectionArrayTmp, selectionArrayCopy[randomIndex]);

            if (!withRepetitions) {
                selectionArrayCopy = ArrayUtils.remove(selectionArrayCopy, randomIndex);
            }
        }

        return Arrays.asList(selectionArrayTmp.clone());
    }

    public static <T> List<T> selectRoundRobin(final List<T> selectionList,
                                                            final int numberOfElements,
                                                            final boolean withRepetitions,
                                                            final boolean reverse, final String id,
                                                            final int steps, final boolean reset) {

        List<List<T>> ts = (List<List<T>>) selectionList;
        T[] selectionArrayCopy = (T[]) ts.toArray();
        T[] selectionArrayTmp = (T[]) new Object[0];

        if (numberOfElements > selectionArrayCopy.length && !withRepetitions) {
            LOG.info("More elements specified: " + numberOfElements + " than available: " + selectionArrayCopy.length);
        }

        if (reverse) {
            ArrayUtils.reverse(selectionArrayCopy);
        }

        AtomicInteger position;

        AtomicInteger atomicInteger = null;
        if (reset) {
            POSITION_MAP.computeIfPresent(id, (key, val) -> new AtomicInteger(0));
        } else {
            atomicInteger = POSITION_MAP.computeIfAbsent(id, a -> new AtomicInteger(0));
        }

        position = atomicInteger;
        LOG.debug("Current position: " + position);

        for (int i = 0; i < numberOfElements; i++) {

            if (i % steps == 0) {
                selectionArrayTmp = ArrayUtils.add(selectionArrayTmp,
                        selectionArrayCopy[position.get() % selectionArrayCopy.length]);
            }

            if (!withRepetitions && i >= selectionArrayCopy.length - 1) {
                LOG.debug("Not processing");
                break;
            }

            LOG.debug("Current element position: " + position.incrementAndGet());

        }

        if (reverse) {
            ArrayUtils.reverse(selectionArrayCopy);
        }

        return Arrays.asList(selectionArrayTmp.clone());

    }

    public static <T> List<T> selectFixed(final List<T> selectionList,
                                                       final List<Integer> elements,
                                                       final boolean withRepetitions) {

        List<List<T>> ts = (List<List<T>>) selectionList;
        T[] selectionArrayCopy = (T[]) ts.toArray();
        T[] selectionArrayTmp = (T[]) new Object[0];

        if (elements.size() > selectionArrayCopy.length && !withRepetitions) {
            LOG.info("More elements specified: " + elements.size() + " than available: " + selectionArrayCopy.length);
        }

        boolean[] isSelected = new boolean[selectionArrayCopy.length];
        for (final Integer element : elements) {
            if (!withRepetitions) {
                    if (!isSelected[element]) {
                        isSelected[element] = true;
                        selectionArrayTmp = ArrayUtils.add(selectionArrayTmp, selectionArrayCopy[element]);
                    } else {
                        LOG.info("Already selected, not selecting " + selectionArrayCopy[element]);
                    }
            } else {
                selectionArrayTmp = ArrayUtils.add(selectionArrayTmp, selectionArrayCopy[element]);
            }
        }

        return Arrays.asList(selectionArrayTmp.clone());
    }

}
