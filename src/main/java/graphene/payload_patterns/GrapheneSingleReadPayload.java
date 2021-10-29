package graphene.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import graphene.configuration.Configuration;
import graphene.payloads.IGrapheneReadPayload;
import graphene.payloads.ReadPayloadType;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GrapheneSingleReadPayload implements IGraphenePayloads {

    private static final Logger LOG = Logger.getLogger(GrapheneSingleReadPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<IGrapheneReadPayload> getPayloads(final E... params) {
        if (params.length == 1) {
            return preparePayloads(params[0]);
        }
        throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
    }

    @SafeVarargs
    @Suspendable
    private final <E> List<IGrapheneReadPayload> preparePayloads(final E... values) {

        List<IGrapheneReadPayload> payLoadList = new ArrayList<>();

        IGrapheneReadPayload iGrapheneReadPayload = null;
        try {
            iGrapheneReadPayload = Configuration.READ_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        Object[] payload = createPayload(
                (ClientObject) values[0]);

        Objects.requireNonNull(iGrapheneReadPayload).setValues(payload);

        iGrapheneReadPayload.setSpecificPayloadType("keyValue");

        payLoadList.add(iGrapheneReadPayload);

        return payLoadList;
    }

    @Suspendable
    private Object[] createPayload(final ClientObject clientObject) {

        List<Object> values = new ArrayList<>();
        values.add(Configuration.READ_PAYLOAD_TYPE);

        if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.JSON_RPC) {
            values.add("get_value_for_key");
            values.add(new String[]{"h9okGhdm703.942513436336308E80.017699250051615767client-0---0"});
            values.add("1");
        } else if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.NODE) {
            values.add(new String[]{"2.38.17"});
        } else if (Configuration.READ_PAYLOAD_TYPE == ReadPayloadType.WALLET) {
            values.add("2.38.17");
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }

        return values.toArray();

    }

}
