package graphene.payload_patterns.sorting;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.google.common.primitives.UnsignedLong;
import cy.agorise.graphenej.Asset;
import cy.agorise.graphenej.AssetAmount;
import graphene.components.UserAccount;
import graphene.configuration.Configuration;
import graphene.payload_patterns.IGraphenePayloads;
import graphene.payloads.IGrapheneWritePayload;
import graphene.payloads.OperationType;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class GrapheneUniformBubblesortPayload implements IGraphenePayloads {

    private static final Logger LOG = Logger.getLogger(GrapheneUniformBubblesortPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<IGrapheneWritePayload> getPayloads(final E... params) {
        if (params.length == 3) {
            return preparePayloads(params[0], String.valueOf(params[1]), params[2]);
        }
        throw new IllegalArgumentException("Expecting exactly 3 arguments for: " + this.getClass().getName());
    }

    @Suspendable
    @SafeVarargs
    private final <E> List<IGrapheneWritePayload> preparePayloads(final E... values) {

        List<IGrapheneWritePayload> payLoadList = new ArrayList<>();

        for (int i = 0; i < (Integer) values[2]; i++) {
            IGrapheneWritePayload iGrapheneWritePayload = null;
            try {
                iGrapheneWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            ImmutablePair<Object[], String> payload = createPayload(
                    (ClientObject) values[0], (String) values[1], i,
                    Objects.requireNonNull(iGrapheneWritePayload));
            Object[] objectList = payload.getLeft();

            Objects.requireNonNull(iGrapheneWritePayload).setValues(objectList);
            Objects.requireNonNull(iGrapheneWritePayload).setSignature(payload.getRight());

            payLoadList.add(iGrapheneWritePayload);
        }

        return payLoadList;
    }

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final String acctId,
                                                          final int i,
                                                          final IGrapheneWritePayload iGrapheneWritePayload) {

        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        int operationId = OperationType.BUBBLESORT_OPERATION.ordinal();

        iGrapheneWritePayload.setBaseOperation(operationId);

        List<String> params = new ArrayList<>();
        int[] randomArr = createRandomArray();
        params.add(Arrays.toString(randomArr));
        params.add(String.valueOf(0));
        params.add(String.valueOf(randomArr.length - 1));
        params.add(signature);

        List<Object> values = new ArrayList<>();
        values.add(operationId);
        values.add("account");
        values.add(new UserAccount(acctId));
        values.add("fee");
        values.add(new AssetAmount(UnsignedLong.valueOf("0"), new Asset("1.3.0")));
        values.add("Function");
        values.add("Sort");
        values.add("Parameters");
        values.add(params);

        iGrapheneWritePayload.setValueToRead(null);
        iGrapheneWritePayload.setEventPrefix("sort/bubbleSort ");
        iGrapheneWritePayload.setSpecificPayloadType("sort/bubblesort");

        return ImmutablePair.of(values.toArray(), signature);

    }

    @Suspendable
    public int[] createRandomArray() {
        int[] arr = new int[Configuration.SORT_ARRAY_LENGTH];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = RandomUtils.nextInt();
        }
        return arr;
    }

}
