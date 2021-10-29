package fabric.payload_patterns.io;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.configuration.Configuration;
import fabric.payload_patterns.IFabricPayloads;
import fabric.payloads.IFabricWritePayload;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class FabricUniformIOWritePayload implements IFabricPayloads {

    private static final Logger LOG = Logger.getLogger(FabricUniformIOWritePayload.class);

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> List<IFabricWritePayload> getPayloads(final E... params) {
        if (params.length == 2) {
            return preparePayloads((ClientObject) params[0], params[1]);
        }
        throw new IllegalArgumentException("Expecting exactly 2 argument for: " + this.getClass().getName());
    }

    @Suspendable
    @SafeVarargs
    private final <E> List<IFabricWritePayload> preparePayloads(final E... values) {

        List<IFabricWritePayload> payLoadList = new ArrayList<>();

        for (int i = 0; i < (Integer) values[1]; i++) {

            IFabricWritePayload iFabricWritePayload = null;
            try {
                iFabricWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            ImmutablePair<Object[], String> payload = createPayload(
                    (ClientObject) values[0], i, Objects.requireNonNull(iFabricWritePayload));
            Object[] objectList = payload.getLeft();

            Objects.requireNonNull(iFabricWritePayload).setValues(objectList);
            Objects.requireNonNull(iFabricWritePayload).setSignature(payload.getRight());

            payLoadList.add(iFabricWritePayload);
        }

        return payLoadList;
    }

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(0);

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final int i,
                                                          final IFabricWritePayload iFabricWritePayload) {

        //int id = REQUEST_COUNTER.incrementAndGet();
        int id = REQUEST_COUNTER.addAndGet(Configuration.SIZE_IO);
        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        // global start id (int) - offset (host) (int) - local id (int)
        //String key = GeneralConfiguration.RUN_ID.split("-")[0] + GeneralConfiguration.RUN_ID.split("-")[1] + id;
        String key = String.valueOf((Integer.parseInt(GeneralConfiguration.RUN_ID.split("-")[0]) + Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[1]) + id));

        String name = "io";
        String function = "Write";

        List<String> argumentList = new ArrayList<>();
        argumentList.add(String.valueOf(Configuration.SIZE_IO));
        argumentList.add(key);
        argumentList.add(String.valueOf(Configuration.RET_LEN_IO));
        argumentList.add(signature);

        String[] arguments = argumentList.toArray(new String[0]);

        List<Object> values = new ArrayList<>();
        values.add(name);
        values.add(function);
        values.add(arguments);

        iFabricWritePayload.setValueToRead(key);
        iFabricWritePayload.setEventPrefix("storage/write ");
        iFabricWritePayload.setSpecificPayloadType("storage/write");

        return ImmutablePair.of(values.toArray(), signature);
    }

}
