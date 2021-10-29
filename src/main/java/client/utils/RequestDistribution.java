package client.utils;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RequestDistribution {

    private static final Map<String, List<Double>> VALUE_MAP = new ConcurrentHashMap<>();
    private static final Map<String, Object> DISTRIBUTION_MAP =
            new ConcurrentHashMap<>();
    private static final Logger LOG = Logger.getLogger(RequestDistribution.class);

    public static void printValueMap() {
        LOG.info(VALUE_MAP);
    }

    @Suspendable
    public static synchronized void writeValueMap() {

        String fileLocation = "E:\\abpes\\";

        for (final Map.Entry<String, List<Double>> stringListEntry : VALUE_MAP.entrySet()) {
            BufferedWriter writer;
            try {
                writer = new BufferedWriter(new FileWriter(fileLocation + stringListEntry.getKey() +
                        "-dc.txt", true));

                for (final Double value : stringListEntry.getValue()) {
                    writer.write(value + System.lineSeparator());
                }
                writer.close();
            } catch (IOException ex) {
                ExceptionHandler.logException(ex);
            }
        }

    }

    @Suspendable
    public static double getSample(final String id) {
        if (DISTRIBUTION_MAP.get(id) == null) {
            LOG.error("Value map entry not existing");
            throw new IllegalArgumentException("Map entry not existing");
        }

        double sample;
        if (DISTRIBUTION_MAP.get(id) instanceof RealDistribution) {
            sample = ((RealDistribution) DISTRIBUTION_MAP.get(id)).sample();
        } else if (DISTRIBUTION_MAP.get(id) instanceof IntegerDistribution) {
            sample = ((IntegerDistribution) DISTRIBUTION_MAP.get(id)).sample();
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }

        VALUE_MAP.computeIfAbsent(id, a -> new ArrayList<>());
        VALUE_MAP.get(id).add(sample);

        return sample;
    }

    @Suspendable
    public static <E> void setDistribution(final String id,
                                                        final E distribution) {

        if (!(distribution instanceof RealDistribution)
                || !(distribution instanceof IntegerDistribution)) {
            throw new IllegalStateException("Trying to put unexpected objects into map");
        }

        /*if (DISTRIBUTION_MAP.get(id) == null) {
            LOG.info("Map entry not existing");
            DISTRIBUTION_MAP.put(id, distribution);
            return;
        }
        LOG.info("Distribution map entry already existing");*/

        if(DISTRIBUTION_MAP.putIfAbsent(id, distribution) != null) {
            LOG.info("Distribution map entry already existing");
        } else {
            LOG.info("Map entry not existing");
        }

    }

    @Suspendable
    public static Object getDistribution(final String id) {
        if (DISTRIBUTION_MAP.get(id) == null) {
            LOG.error("Distribution map entry not existing");
            return null;
        }
        if (!(DISTRIBUTION_MAP.get(id) instanceof RealDistribution)
                || !(DISTRIBUTION_MAP.get(id) instanceof IntegerDistribution)) {
            throw new IllegalStateException("Unexpected objects within map");
        }
        return DISTRIBUTION_MAP.get(id);
    }

}
