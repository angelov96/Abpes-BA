package graphene.payload_patterns.io;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class GrapheneUniformIORevertScanPayload implements IGraphenePayloads {

    private static final Logger LOG = Logger.getLogger(GrapheneUniformIORevertScanPayload.class);

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

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(GeneralConfiguration.CLIENT_COUNT * GeneralConfiguration.CLIENT_WORKLOADS.get(0) *
            Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT * Configuration.SIZE_IO);

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final String acctId,
                                                          final int i,
                                                          final IGrapheneWritePayload iGrapheneWritePayload) {

        //int id = REQUEST_COUNTER.getAndDecrement();
        int id = REQUEST_COUNTER.getAndAdd(-Configuration.SIZE_IO);
        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        // global start id (int) - offset (host) (int) - local id (int)
        //String key = GeneralConfiguration.RUN_ID.split("-")[0] + GeneralConfiguration.RUN_ID.split("-")[1] + id;
        String key = String.valueOf((Integer.parseInt(GeneralConfiguration.RUN_ID.split("-")[0]) + Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[1]) + id));

        int operationId = OperationType.IO_IN_OPERATION.ordinal();

        iGrapheneWritePayload.setBaseOperation(operationId);

        List<String> params = new ArrayList<>();
        params.add(String.valueOf(Configuration.SIZE_IO));
        params.add(key);
        params.add(signature);

        List<Object> values = new ArrayList<>();
        values.add(operationId);
        values.add("account");
        values.add(new UserAccount(acctId));
        values.add("fee");
        values.add(new AssetAmount(UnsignedLong.valueOf("0"), new Asset("1.3.0")));
        values.add("Function");
        values.add("RevertScan");
        values.add("Parameters");
        values.add(params);

        iGrapheneWritePayload.setValueToRead(null);
        iGrapheneWritePayload.setEventPrefix("storage/revertScan ");
        iGrapheneWritePayload.setSpecificPayloadType("storage/revertScan");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
