package fabric.payload_patterns.sorting;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.configuration.Configuration;
import fabric.payload_patterns.IFabricPayloads;
import fabric.payloads.IFabricWritePayload;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FabricUniformBubblesortPayload implements IFabricPayloads {

    private static final Logger LOG = Logger.getLogger(FabricUniformBubblesortPayload.class);

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

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final int i,
                                                          final IFabricWritePayload iFabricWritePayload) {

        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        String name = "bubbleSort";
        String function = "Sort";

        ImmutablePair<List<String>, Integer> randomArrayPair = createRandomListAsPair();

        List<String> argumentList = new ArrayList<>(randomArrayPair.getLeft());
        argumentList.add(String.valueOf(0));
        argumentList.add(String.valueOf(randomArrayPair.getRight() - 1));
        argumentList.add(signature);

        String[] arguments = argumentList.toArray(new String[0]);

        List<Object> values = new ArrayList<>();
        values.add(name);
        values.add(function);
        values.add(arguments);

        iFabricWritePayload.setValueToRead(null);
        iFabricWritePayload.setEventPrefix("sort/bubbleSort ");
        iFabricWritePayload.setSpecificPayloadType("sort/bubbleSort");

        return ImmutablePair.of(values.toArray(), signature);
    }

    @Suspendable
    public ImmutablePair<List<String>, Integer> createRandomListAsPair() {
        List<String> stringList = new ArrayList<>();
        for (int i = 0; i < Configuration.SORT_ARRAY_LENGTH; i++) {
            stringList.add(String.valueOf(RandomUtils.nextInt()));
        }
        return ImmutablePair.of(stringList, stringList.size());
    }

}
