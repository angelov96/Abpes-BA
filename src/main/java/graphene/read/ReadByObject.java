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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ReadByObject extends BaseGrapheneHandler implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(ReadByObject.class);

    private static final int LOGIN_ID = 2;
    private static final int DATABASE_ID = 3;
    private static final int OBJECT_CALLBACK_ID = 4;
    private static final List<Serializable> EMPTY_PARAMS = new ArrayList<>();
    private final List<Serializable> objectIds;
    private final WitnessResponseListener listener;
    private final boolean oneTime;
    private final ReadStatisticObject readStatisticObject;
    private final CompletableFuture<ImmutablePair<Boolean, String>> done = new CompletableFuture<>();
    private int currentId = 1;

    public ReadByObject(final boolean oneTimeConstructor, final WitnessResponseListener listenerConstructor,
                        final List<Serializable> objectIdsConstructor,
                        final ReadStatisticObject readStatisticObjectConstructor) {
        super(listenerConstructor);
        this.oneTime = oneTimeConstructor;
        this.listener = listenerConstructor;
        this.objectIds = objectIdsConstructor;
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
        ArrayList<Serializable> loginParams = new ArrayList<>();
        loginParams.add(Configuration.RPC_USERNAME_NODE);
        loginParams.add(Configuration.RPC_PASSWORD_NODE);
        ApiCall loginCall = new ApiCall(ApiId.ACCESS_RESTRICTED_API.ordinal(), RPC.CALL_LOGIN, loginParams,
                RPC.VERSION, currentId);
        websocket.sendText(loginCall.toJsonString());
    }

    @Suspendable
    @Override
    public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) throws Exception {
        if (frame.isTextFrame()) {
            for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                if (frame.getPayloadText().contains(errorMessage)) {
                    readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    done.complete(ImmutablePair.of(true, errorMessage));
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

            if (currentId == LOGIN_ID) {
                ApiCall database = new ApiCall(ApiId.ACCESS_RESTRICTED_API.ordinal(), "database", EMPTY_PARAMS,
                        RPC.VERSION, currentId);
                websocket.sendText(database.toJsonString());
            }
            if (currentId == DATABASE_ID) {
                ApiCall objectCallback = new ApiCall(ApiId.FOLLOW_UP_RESTRICTED_API.ordinal(),
                        "get_objects", objectIds, RPC.VERSION,
                        currentId);
                websocket.sendText(objectCallback.toJsonString());
                if (!Configuration.RECEIVE_READ_REQUEST) {
                    readStatisticObject.setEndTime(-1);
                    done.complete(ImmutablePair.of(false, "Sent async"));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }
            }
            if (currentId == OBJECT_CALLBACK_ID) {
                LOG.info("Done " + frame.getPayloadText());
                readStatisticObject.setEndTime(System.nanoTime());
                done.complete(ImmutablePair.of(false, frame.getPayloadText()));
                if (oneTime) {
                    websocket.disconnect();
                }
            }
            if (currentId > OBJECT_CALLBACK_ID || currentId < LOGIN_ID) {
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
            LOG.info("GetObjectsRequest sent: " + frame.getPayloadText());
        }
    }

    @Suspendable
    @Override
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("GetObjectsRequest error, cause: " + cause.getMessage());
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
        LOG.error("GetObjectsRequest handleCallbackError, cause: " + cause.getMessage() + ", error: " + cause.getClass());
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
            List<Serializable> objectIdsConstructor = (List<Serializable>) params[2];
            ReadStatisticObject readStatisticObjectConstructor = (ReadStatisticObject) params[3];

            return (E) new ReadByObject(oneTimeConstructor, listenerConstructor, objectIdsConstructor,
                    readStatisticObjectConstructor);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

}
