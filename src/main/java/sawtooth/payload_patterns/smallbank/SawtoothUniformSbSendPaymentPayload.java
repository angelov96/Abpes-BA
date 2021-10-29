package sawtooth.payload_patterns.smallbank;

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

public class SawtoothUniformSbSendPaymentPayload implements ISawtoothPayloads {

    private static final Logger LOG = Logger.getLogger(SawtoothUniformSbSendPaymentPayload.class);

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

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(0);

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final int i,
                                                          final ISawtoothWritePayload iSawtoothWritePayload) {

        int id = REQUEST_COUNTER.updateAndGet(value -> (value % Configuration.SEND_CYCLE == 0) ? 1 : value + 1);

        if(Configuration.SEND_CYCLE > (GeneralConfiguration.CLIENT_COUNT * GeneralConfiguration.CLIENT_WORKLOADS.get(0)
                * Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT * Configuration.NUMBER_OF_BATCHES_PER_CLIENT)) {
            LOG.error("More cycles than accounts available, aborting");
            System.exit(1);
        }

        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        // global start id (string) - offset (host) (string) - local id (int)
        String prefix_sender;
        String prefix_destination;

        if(id % Configuration.SEND_CYCLE == 0) {
            prefix_sender = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;
            prefix_destination = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + 1;
            LOG.debug("Reset id destination: " + prefix_destination + " sender: " + prefix_sender);
        } else {
            prefix_sender = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;
            prefix_destination = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + (id + 1);
        }

        String function = "SendPayment";

        List<String> argumentList = new ArrayList<>();
        argumentList.add(prefix_sender);
        argumentList.add(prefix_destination);
        argumentList.add(String.valueOf(Configuration.SEND_PAYMENT_AMOUNT));
        argumentList.add(signature);

        String[] params = argumentList.toArray(new String[0]);

        List<Object> values = new ArrayList<>();
        values.add(function);
        values.add(params);

        iSawtoothWritePayload.setValueToRead(null);
        iSawtoothWritePayload.setFamilyName("smallBank");
        iSawtoothWritePayload.setFamilyVersion("0.1");
        iSawtoothWritePayload.setEventPrefix("sendPayment ");
        iSawtoothWritePayload.setSpecificPayloadType("sendPayment");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
