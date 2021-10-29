package quorum.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.Utf8String;
import quorum.configuration.Configuration;
import quorum.payloads.IQuorumReadPayload;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QuorumSingleReadPayload implements IQuorumPayloads {

    private static final Logger LOG = Logger.getLogger(QuorumSingleReadPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<IQuorumReadPayload> getPayloads(final E... params) {
        if (params.length == 1) {
            return preparePayloads((ClientObject) params[0]);
        }
        throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<IQuorumReadPayload> preparePayloads(final E... values) {

        List<IQuorumReadPayload> payLoadList = new ArrayList<>();

        IQuorumReadPayload iQuorumReadPayload = null;
        try {
            iQuorumReadPayload = Configuration.READ_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        Object[] payload = createPayload(
                (ClientObject) values[0]);

        Objects.requireNonNull(iQuorumReadPayload).setValues(payload);

        iQuorumReadPayload.setSpecificPayloadType("keyValue");

        payLoadList.add(iQuorumReadPayload);

        return payLoadList;
    }

    @Suspendable
    private Object[] createPayload(final ClientObject clientObject) {

        String key = "6IvfjhEwR12.76323897567368E80.11252445733990368client-0---0";
        String function = "get";
        List<Type<?>> inputs = new ArrayList<>();
        inputs.add(new Utf8String(key));
        inputs.add(new Utf8String("sigx"));
        List<TypeReference<?>> outputs = new ArrayList<>();
        outputs.add(TypeReference.create(Utf8String.class));

        List<Object> values = new ArrayList<>();
        values.add(function);
        values.add(inputs);
        values.add(outputs);

        return values.toArray();

    }

}
