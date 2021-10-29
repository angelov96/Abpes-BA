package sawtooth.payload_patterns.loop;

import client.client.ClientObject;
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

public class SawtoothUniformLoopPayload implements ISawtoothPayloads {

    private static final Logger LOG = Logger.getLogger(SawtoothUniformLoopPayload.class);

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

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final int i,
                                                          final ISawtoothWritePayload iSawtoothWritePayload) {

        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        List<String> argumentList = new ArrayList<>();
        argumentList.add(String.valueOf(Configuration.START_LOOP));
        argumentList.add(String.valueOf(Configuration.END_LOOP));
        argumentList.add(signature);

        String[] params = argumentList.toArray(new String[0]);

        List<Object> values = new ArrayList<>();
        values.add("Loop");
        values.add(params);

        iSawtoothWritePayload.setValueToRead(null);
        iSawtoothWritePayload.setFamilyName("loop");
        iSawtoothWritePayload.setFamilyVersion("0.1");
        iSawtoothWritePayload.setEventPrefix("loop ");
        iSawtoothWritePayload.setSpecificPayloadType("loop");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
