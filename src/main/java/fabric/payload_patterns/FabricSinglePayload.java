package fabric.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.configuration.Configuration;
import fabric.payloads.IFabricWritePayload;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FabricSinglePayload implements IFabricPayloads {

    private static final Logger LOG = Logger.getLogger(FabricSinglePayload.class);

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> List<IFabricWritePayload> getPayloads(final E... params) {
        if (params.length == 1) {
            return preparePayloads((ClientObject) params[0]);
        }
        throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<IFabricWritePayload> preparePayloads(final E... values) {

        List<IFabricWritePayload> payLoadList = new ArrayList<>();

        IFabricWritePayload iFabricWritePayload = null;
        try {
            iFabricWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        ImmutablePair<Object[], String> payload = createPayload(
                (ClientObject) values[0], Objects.requireNonNull(iFabricWritePayload));
        Object[] objectList = payload.getLeft();

        Objects.requireNonNull(iFabricWritePayload).setValues(objectList);
        Objects.requireNonNull(iFabricWritePayload).setSignature(payload.getRight());

        payLoadList.add(iFabricWritePayload);

        return payLoadList;
    }

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject,
                                                          final IFabricWritePayload iFabricWritePayload) {

        String signature = clientObject.getClientId() + "---";
        String key =
                RandomStringUtils.random(10, true, true) + ((Math.random() * (1000000000 - 0)) + 0) + Math.random();

        String name = "key_value";
        String function = "Set";
        String[] arguments = new String[]{key, RandomStringUtils.random(Configuration.KEY_VALUE_STRING_LENGTH, true, true), signature};
        List<Object> values = new ArrayList<>();
        values.add(name);
        values.add(function);
        values.add(arguments);

        iFabricWritePayload.setValueToRead(key);
        iFabricWritePayload.setEventPrefix("keyValue/set ");
        iFabricWritePayload.setSpecificPayloadType("keyValue");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
