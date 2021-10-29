package graphene.listener;

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
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GrapheneUnsubscription extends BaseGrapheneHandler {

    private static final Logger LOG = Logger.getLogger(GrapheneUnsubscription.class);

    private static final int LOGIN_ID = 2;
    private static final int DATABASE_ID = 3;
    private static final int CANCEL_SUBSCRIPTIONS = 4;
    private static final List<Serializable> EMPTY_PARAMS = new ArrayList<>();
    private final WitnessResponseListener listener;
    private final CompletableFuture<Boolean> isUnsubscribed = new CompletableFuture<>();
    private final boolean oneTime;
    private int currentId = 1;

    public GrapheneUnsubscription(final boolean oneTimeConstructor, final WitnessResponseListener listenerConstructor) {
        super(listenerConstructor);
        this.oneTime = oneTimeConstructor;
        this.listener = listenerConstructor;
    }

    @Suspendable
    public CompletableFuture<Boolean> getIsUnsubscribed() {
        return isUnsubscribed;
    }

    @Suspendable
    @Override
    public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
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
            /*for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                if (frame.getPayloadText().contains(errorMessage)) {
                    throw new Exception(frame.getPayloadText());
                }
            }*/
            LOG.trace("Unsubscription received: " + frame.getPayloadText());
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
                ApiCall unsubscriptionCallback = new ApiCall(ApiId.FOLLOW_UP_RESTRICTED_API.ordinal(),
                        "cancel_all_subscriptions", EMPTY_PARAMS,
                        RPC.VERSION, currentId);
                websocket.sendText(unsubscriptionCallback.toJsonString());
            }
            if (currentId == CANCEL_SUBSCRIPTIONS) {
                LOG.info("Unsubscribed");
                isUnsubscribed.complete(true);
                if (oneTime) {
                    websocket.disconnect();
                }
            } else {
                LOG.error("Received unexpected message: " + frame.getPayloadText());
            }

        }
    }

    @Suspendable
    @Override
    public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
        if (frame.isTextFrame()) {
            LOG.info("Subscription: " + frame.getPayloadText());
        }
    }

    @Suspendable
    @Override
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("Subscription error, cause: " + cause.getMessage());
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

    @Suspendable
    @Override
    public void handleCallbackError(final WebSocket websocket, final Throwable cause) {
        LOG.error("Subscription handleCallbackError, cause: " + cause.getMessage());
        for (final StackTraceElement element : cause.getStackTrace()) {
            LOG.error(element.getFileName() + "#" + element.getClassName() + ":" + element.getLineNumber());
        }
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

}
