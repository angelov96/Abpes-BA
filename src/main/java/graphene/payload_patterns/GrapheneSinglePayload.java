package graphene.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.google.common.primitives.UnsignedLong;
import cy.agorise.graphenej.Asset;
import cy.agorise.graphenej.AssetAmount;
import graphene.components.UserAccount;
import graphene.configuration.Configuration;
import graphene.payloads.IGrapheneWritePayload;
import graphene.payloads.OperationType;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GrapheneSinglePayload implements IGraphenePayloads {

    private static final Logger LOG = Logger.getLogger(GrapheneSinglePayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<IGrapheneWritePayload> getPayloads(final E... params) {
        if (params.length == 2) {
            return preparePayloads(params[0], String.valueOf(params[1]));
        }
        throw new IllegalArgumentException("Expecting exactly 2 arguments for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<IGrapheneWritePayload> preparePayloads(final E... values) {

        List<IGrapheneWritePayload> payLoadList = new ArrayList<>();

        IGrapheneWritePayload iGrapheneWritePayload = null;
        try {
            iGrapheneWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        ImmutablePair<Object[], String> payload = createPayload(
                (ClientObject) values[0], (String) values[1],
                Objects.requireNonNull(iGrapheneWritePayload));
        Object[] objectList = payload.getLeft();

        Objects.requireNonNull(iGrapheneWritePayload).setValues(objectList);
        Objects.requireNonNull(iGrapheneWritePayload).setSignature(payload.getRight());

        payLoadList.add(iGrapheneWritePayload);

        return payLoadList;
    }

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final String acctId,
                                                          final IGrapheneWritePayload iGrapheneWritePayload) {

        String signature = clientObject.getClientId() + "---" + System.nanoTime();
        String key =
                RandomStringUtils.random(10, true, true) + ((Math.random() * (1000000000 - 0)) + 0) + Math.random();

        List<String> params = new ArrayList<>();
        params.add(key);
        params.add(RandomStringUtils.random(Configuration.KEY_VALUE_STRING_LENGTH, true, true));
        params.add(signature);

        int operationId = OperationType.KEY_VALUE_IN_OPERATION.ordinal();

        iGrapheneWritePayload.setBaseOperation(operationId);

        List<Object> values = new ArrayList<>();
        values.add(operationId);
        values.add("account");
        values.add(new UserAccount(acctId));
        values.add("fee");
        values.add(new AssetAmount(UnsignedLong.valueOf("0"), new Asset("1.3.0")));
        values.add("Function");
        values.add("Set");
        values.add("Parameters");
        values.add(params);

        iGrapheneWritePayload.setValueToRead(key);
        iGrapheneWritePayload.setEventPrefix("keyValue/set ");
        iGrapheneWritePayload.setSpecificPayloadType("keyValue");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
