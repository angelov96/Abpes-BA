package fabric.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.configuration.Configuration;
import fabric.payloads.IFabricReadPayload;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FabricSingleReadPayload implements IFabricPayloads {

    private static final Logger LOG = Logger.getLogger(FabricSingleReadPayload.class);

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> List<IFabricReadPayload> getPayloads(final E... params) {
        if (params.length == 1) {
            return preparePayloads((ClientObject) params[0]);
        }
        throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
    }

    @Suspendable
    @SafeVarargs
    private final <E> List<IFabricReadPayload> preparePayloads(final E... values) {

        List<IFabricReadPayload> payLoadList = new ArrayList<>();

        IFabricReadPayload iFabricReadPayload = null;
        try {
            iFabricReadPayload = Configuration.READ_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        Object[] payload = createPayload(
                (ClientObject) values[0]);

        Objects.requireNonNull(iFabricReadPayload).setValues(payload);

        payLoadList.add(iFabricReadPayload);

        iFabricReadPayload.setSpecificPayloadType("keyValue");

        return payLoadList;
    }

    @Suspendable
    private Object[] createPayload(final ClientObject clientObject) {

        String signature = clientObject.getClientId() + "---";

        String name = "racecondition";
        String function = "Get";
        String[] arguments = new String[]{"MXerE1LXU99.778324288377235E80.761143696282622client-0---0", signature};
        List<Object> values = new ArrayList<>();
        values.add(name);
        values.add(function);
        values.add(arguments);
        return values.toArray();

    }

}
