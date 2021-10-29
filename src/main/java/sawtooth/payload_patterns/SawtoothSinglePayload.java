package sawtooth.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import sawtooth.configuration.Configuration;
import sawtooth.payloads.ISawtoothWritePayload;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SawtoothSinglePayload implements ISawtoothPayloads {

    private static final Logger LOG = Logger.getLogger(SawtoothSinglePayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<ISawtoothWritePayload> getPayloads(final E... params) {
        if (params.length == 1) {
            return preparePayloads((ClientObject) params[0]);
        }
        throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<ISawtoothWritePayload> preparePayloads(final E... values) {
        List<ISawtoothWritePayload> payLoadList = new ArrayList<>();

        ISawtoothWritePayload iSawtoothWritePayload = null;
        try {
            iSawtoothWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        ImmutablePair<Object[], String> payload = createPayload(
                (ClientObject) values[0], Objects.requireNonNull(iSawtoothWritePayload));
        Object[] objectList = payload.getLeft();

        Objects.requireNonNull(iSawtoothWritePayload).setValues(objectList);
        Objects.requireNonNull(iSawtoothWritePayload).setSignature(payload.getRight());

        payLoadList.add(iSawtoothWritePayload);

        return payLoadList;
    }

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject,
                                                          final ISawtoothWritePayload iSawtoothWritePayload) {
        String signature = clientObject.getClientId() + "---";
        String key =
                RandomStringUtils.random(10, true, true) + ((Math.random() * (1000000000 - 0)) + 0) + Math.random();

        String function = "Set";
        String[] params = new String[]{key, RandomStringUtils.random(Configuration.KEY_VALUE_STRING_LENGTH, true, true), signature};

        List<Object> values = new ArrayList<>();
        values.add(function);
        values.add(params);

        iSawtoothWritePayload.setValueToRead(key);
        iSawtoothWritePayload.setFamilyName("keyValue");
        iSawtoothWritePayload.setFamilyVersion("0.1");
        iSawtoothWritePayload.setEventPrefix("keyValue/set ");
        iSawtoothWritePayload.setSpecificPayloadType("keyValue");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
