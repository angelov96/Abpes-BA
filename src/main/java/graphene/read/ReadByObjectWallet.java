package graphene.read;

import client.configuration.GeneralConfiguration;
import co.paralleluniverse.fibers.Suspendable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.api.BaseGrapheneHandler;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.ApiCall;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.DynamicGlobalProperties;
import graphene.configuration.ApiId;
import graphene.configuration.Configuration;
import graphene.statistics.ReadStatisticObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ReadByObjectWallet extends BaseGrapheneHandler implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(ReadByObjectWallet.class);

    private static final int RESPONSE_ID = 2;
    private final WitnessResponseListener listener;
    private final boolean oneTime;
    private final ReadStatisticObject readStatisticObject;
    private final String objectId;
    private final CompletableFuture<ImmutablePair<Boolean, String>> done = new CompletableFuture<>();
    private int currentId = 1;

    public ReadByObjectWallet(final boolean oneTimeConstructor, final WitnessResponseListener listenerConstructor,
                              final String objectIdConstructor,
                              final ReadStatisticObject readStatisticObjectConstructor) {
        super(listenerConstructor);
        this.oneTime = oneTimeConstructor;
        this.listener = listenerConstructor;
        this.objectId = objectIdConstructor;
        this.readStatisticObject = readStatisticObjectConstructor;
    }

    @Suspendable
    public CompletableFuture<ImmutablePair<Boolean, String>> getDone() {
        return done;
    }

    @Suspendable
    @Override
    public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
        readStatisticObject.setStartTime(System.nanoTime());
        List<Serializable> objectIdList = Collections.singletonList(objectId);
        ApiCall getObjectCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "get_object", objectIdList,
                RPC.VERSION, currentId);
        websocket.sendText(getObjectCall.toJsonString());
        if (!Configuration.RECEIVE_READ_REQUEST) {
            readStatisticObject.setEndTime(-1);
            done.complete(ImmutablePair.of(false, "Sent async"));
        }
    }

    @Suspendable
    @Override
    public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) throws Exception {
        if (frame.isTextFrame()) {
            for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                if (frame.getPayloadText().contains(errorMessage)) {
                    readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    done.complete(ImmutablePair.of(true, frame.getPayloadText()));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }
            }
            LOG.info("GetObjectRequest received: " + frame.getPayloadText());
        }
        String response = frame.getPayloadText();
        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DynamicGlobalProperties.class,
                new DynamicGlobalProperties.DynamicGlobalPropertiesDeserializer());
        Gson gson = builder.create();
        BaseResponse baseResponse = gson.fromJson(response, BaseResponse.class);
        if (baseResponse.error != null) {
            listener.onError(baseResponse.error);
            if (oneTime) {
                websocket.disconnect();
            }
        } else {
            currentId++;
            if (currentId == RESPONSE_ID) {
                LOG.info("Done");
                readStatisticObject.setEndTime(System.nanoTime());
                done.complete(ImmutablePair.of(false, frame.getPayloadText()));
                if (oneTime) {
                    websocket.disconnect();
                }
            }
            if (currentId > RESPONSE_ID || currentId < RESPONSE_ID) {
                LOG.error("Received unexpected message");
                readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                done.complete(ImmutablePair.of(true, "Received unexpected message"));
                if (oneTime) {
                    websocket.disconnect();
                }
            }
        }
    }

    @Suspendable
    @Override
    public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
        if (frame.isTextFrame()) {
            LOG.info("GetObjectRequest sent: " + frame.getPayloadText());
        }
    }

    @Suspendable
    @Override
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("GetObjectRequest error, cause: " + cause.getMessage());
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
        done.complete(ImmutablePair.of(true, cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

    @Suspendable
    @Override
    public void handleCallbackError(final WebSocket websocket, final Throwable cause) {
        LOG.error("GetObjectRequest handleCallbackError, cause: " + cause.getMessage() + ", error: " + cause.getClass());
        for (final StackTraceElement element : cause.getStackTrace()) {
            LOG.error(element.getFileName() + "#" + element.getClassName() + ":" + element.getLineNumber());
        }
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
        done.complete(ImmutablePair.of(true, cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E read(final E... params) {
        if (params.length == 4) {

            boolean oneTimeConstructor = Boolean.parseBoolean((String) params[0]);
            WitnessResponseListener listenerConstructor = (WitnessResponseListener) params[1];
            String objectIdConstructor = (String) params[2];
            ReadStatisticObject readStatisticObjectConstructor = (ReadStatisticObject) params[3];

            return (E) new ReadByObjectWallet(oneTimeConstructor, listenerConstructor, objectIdConstructor,
                    readStatisticObjectConstructor);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }
}
