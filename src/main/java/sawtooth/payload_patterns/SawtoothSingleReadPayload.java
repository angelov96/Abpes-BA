package sawtooth.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;
import sawtooth.configuration.Configuration;
import sawtooth.payloads.ISawtoothReadPayload;
import sawtooth.payloads.TpEnum;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SawtoothSingleReadPayload implements ISawtoothPayloads {

    private static final Logger LOG = Logger.getLogger(SawtoothSingleReadPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<ISawtoothReadPayload> getPayloads(final E... params) {
        if (params.length == 1) {
            return preparePayloads((ClientObject) params[0]);
        }
        throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<ISawtoothReadPayload> preparePayloads(final E... values) {
        List<ISawtoothReadPayload> payLoadList = new ArrayList<>();

        ISawtoothReadPayload iSawtoothReadPayload = null;
        try {
            iSawtoothReadPayload = Configuration.READ_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        Object[] payload = createPayload(
                (ClientObject) values[0]);

        Objects.requireNonNull(iSawtoothReadPayload).setValues(payload);

        iSawtoothReadPayload.setSpecificPayloadType("keyValue");

        payLoadList.add(iSawtoothReadPayload);

        return payLoadList;
    }

    @Suspendable
    private Object[] createPayload(final ClientObject clientObject) {
        String valueToRead = "JpFpOb1QSR7.275900601049988E80.6756071996137201client-0---0";
        TpEnum tpEnum = Configuration.TP_ENUM;
        String tpPrefix = Configuration.TP_PREFIX;

        List<Object> values = new ArrayList<>();
        values.add(valueToRead);
        values.add(tpEnum);

        return values.toArray();

    }

}
