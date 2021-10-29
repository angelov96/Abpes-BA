package sawtooth.payload_patterns.io;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import sawtooth.configuration.Configuration;
import sawtooth.payload_patterns.ISawtoothPayloads;
import sawtooth.payloads.ISawtoothWritePayload;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class SawtoothUniformIORevertScanPayload implements ISawtoothPayloads {

    private static final Logger LOG = Logger.getLogger(SawtoothUniformIORevertScanPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<ISawtoothWritePayload> getPayloads(final E... params) {
        if (params.length == 2) {
            return preparePayloads((ClientObject) params[0], params[1]);
        }
        throw new IllegalArgumentException("Expecting exactly 2 arguments for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<ISawtoothWritePayload> preparePayloads(final E... values) {
        List<ISawtoothWritePayload> payLoadList = new ArrayList<>();

        for (int i = 0; i < (Integer) values[1]; i++) {

            ISawtoothWritePayload iSawtoothWritePayload = null;
            try {
                iSawtoothWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            ImmutablePair<Object[], String> payload = createPayload(
                    (ClientObject) values[0], i, Objects.requireNonNull(iSawtoothWritePayload));
            Object[] objectList = payload.getLeft();

            Objects.requireNonNull(iSawtoothWritePayload).setValues(objectList);
            Objects.requireNonNull(iSawtoothWritePayload).setSignature(payload.getRight());

            payLoadList.add(iSawtoothWritePayload);
        }

        return payLoadList;
    }

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(GeneralConfiguration.CLIENT_COUNT * GeneralConfiguration.CLIENT_WORKLOADS.get(0) *
            Configuration.NUMBER_OF_BATCHES_PER_CLIENT * Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT * Configuration.SIZE_IO);

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final int i,
                                                          final ISawtoothWritePayload iSawtoothWritePayload) {

        //int id = REQUEST_COUNTER.getAndDecrement();
        int id = REQUEST_COUNTER.getAndAdd(-Configuration.SIZE_IO);
        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        // global start id (int) - offset (host) (int) - local id (int)
        //String key = GeneralConfiguration.RUN_ID.split("-")[0] + GeneralConfiguration.RUN_ID.split("-")[1] + id;
        String key = String.valueOf((Integer.parseInt(GeneralConfiguration.RUN_ID.split("-")[0]) + Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[1]) + id));

        String function = "RevertScan";

        List<String> argumentList = new ArrayList<>();
        argumentList.add(String.valueOf(Configuration.SIZE_IO));
        argumentList.add(key);
        argumentList.add(signature);

        String[] params = argumentList.toArray(new String[0]);

        List<Object> values = new ArrayList<>();
        values.add(function);
        values.add(params);

        iSawtoothWritePayload.setValueToRead(null);
        iSawtoothWritePayload.setFamilyName("io");
        iSawtoothWritePayload.setFamilyVersion("0.1");
        iSawtoothWritePayload.setEventPrefix("storage/revertScan ");
        iSawtoothWritePayload.setSpecificPayloadType("storage/revertScan");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
