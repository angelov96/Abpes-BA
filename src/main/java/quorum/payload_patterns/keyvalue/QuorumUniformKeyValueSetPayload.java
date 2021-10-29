package quorum.payload_patterns.keyvalue;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.Utf8String;
import quorum.configuration.Configuration;
import quorum.payload_patterns.IQuorumPayloads;
import quorum.payloads.IQuorumWritePayload;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class QuorumUniformKeyValueSetPayload implements IQuorumPayloads {

    private static final Logger LOG = Logger.getLogger(QuorumUniformKeyValueSetPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<IQuorumWritePayload> getPayloads(final E... params) {
        if (params.length == 2) {
            return preparePayloads((ClientObject) params[0], params[1]);
        }
        throw new IllegalArgumentException("Expecting exactly 2 arguments for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<IQuorumWritePayload> preparePayloads(final E... values) {

        List<IQuorumWritePayload> payLoadList = new ArrayList<>();

        for (int i = 0; i < (Integer) values[1]; i++) {
            IQuorumWritePayload iQuorumWritePayload = null;
            try {
                iQuorumWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            ImmutablePair<Object[], String> payload = createPayload(
                    (ClientObject) values[0], i, Objects.requireNonNull(iQuorumWritePayload));
            Object[] objectList = payload.getLeft();

            Objects.requireNonNull(iQuorumWritePayload).setValues(objectList);
            Objects.requireNonNull(iQuorumWritePayload).setSignature(payload.getRight());

            payLoadList.add(iQuorumWritePayload);
        }

        return payLoadList;
    }

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(0);

    @Suspendable
    private ImmutablePair<Object[], String> createPayload(final ClientObject clientObject, final int i,
                                                          final IQuorumWritePayload iQuorumWritePayload) {

        int id = REQUEST_COUNTER.incrementAndGet();
        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);
        String key = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;

        String function = "set";
        List<Type<?>> inputs = new ArrayList<>();
        inputs.add(new Utf8String(key));

        inputs.add(new Utf8String(RandomStringUtils.random(Configuration.KEY_VALUE_STRING_LENGTH, true, true)));
        inputs.add(new Utf8String(signature));
        List<TypeReference<?>> outputs = new ArrayList<>();
        //outputs.add(TypeReference.create(Utf8String.class));

        List<Object> values = new ArrayList<>();
        values.add(function);
        values.add(inputs);
        values.add(outputs);

        iQuorumWritePayload.setValueToRead(key);
        iQuorumWritePayload.setEventPrefix("keyValue/set");
        iQuorumWritePayload.setSpecificPayloadType("keyValue/set");

        return ImmutablePair.of(values.toArray(), signature);

    }

}
