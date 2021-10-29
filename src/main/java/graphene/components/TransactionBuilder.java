package graphene.components;

import client.configuration.GeneralConfiguration;
import co.paralleluniverse.fibers.Suspendable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.api.BaseGrapheneHandler;
import cy.agorise.graphenej.interfaces.JsonSerializable;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.ApiCall;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.DynamicGlobalProperties;
import graphene.configuration.ApiId;
import graphene.configuration.Configuration;
import graphene.statistics.WriteStatisticObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TransactionBuilder extends BaseGrapheneHandler {

    private static final Logger LOG = Logger.getLogger(TransactionBuilder.class);
    private static final int BUILDER_RESPONSE = 2;
    private static final int OPERATION_ADDITION_RESPONSE = 3;
    private static final int FEE_RESPONSE = 4;
    private static final int SIGN_RESPONSE = 5;

    private final Transaction transaction;
    private final WitnessResponseListener listener;
    private final boolean oneTime;
    private final CompletableFuture<ImmutablePair<Boolean, String>> responseFuture = new CompletableFuture<>();

    private final WriteStatisticObject writeStatisticObject;

    private int currentId = 1;
    private int builderNumber = -1;

    public TransactionBuilder(final Transaction transaction, final WitnessResponseListener listener,
                              final WriteStatisticObject writeStatisticObjectConstructor) {
        this(transaction, true, listener, writeStatisticObjectConstructor);
    }

    public TransactionBuilder(final Transaction transaction, final boolean oneTime,
                              final WitnessResponseListener listener,
                              final WriteStatisticObject writeStatisticObjectConstructor) {
        super(listener);
        this.transaction = transaction;
        this.oneTime = oneTime;
        this.listener = listener;
        this.writeStatisticObject = writeStatisticObjectConstructor;
    }

    @Suspendable
    public CompletableFuture<ImmutablePair<Boolean, String>> getResponseFuture() {
        return responseFuture;
    }

    @Suspendable
    @Override
    public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
        ArrayList<Serializable> loginParams = new ArrayList<>();
        ApiCall loginCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "begin_builder_transaction", loginParams,
                RPC.VERSION, currentId);
        websocket.sendText(loginCall.toJsonString());
    }

    @Suspendable
    @Override
    public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) throws Exception {
        if (frame.isTextFrame()) {
            for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                if (frame.getPayloadText().contains(errorMessage)) {
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    responseFuture.complete(ImmutablePair.of(true, frame.getPayloadText()));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }
            }
            LOG.info("Builder text: " + frame.getPayloadText());
        }

        boolean offset = (transaction.getOperations().size() > 1);
        ++currentId;

        GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(DynamicGlobalProperties.class,
                new DynamicGlobalProperties.DynamicGlobalPropertiesDeserializer());
        Gson gson = builder.create();
        String response = frame.getPayloadText();
        BaseResponse baseResponse = gson.fromJson(response, BaseResponse.class);
        if (baseResponse.error != null) {
            listener.onError(baseResponse.error);
            if (oneTime) {
                websocket.disconnect();
            }
        } else {
            if (currentId == BUILDER_RESPONSE) {
                String jsonString = frame.getPayloadText();
                JSONObject jsonObject = new JSONObject(jsonString);
                builderNumber = jsonObject.getInt("result");

                LOG.info("Set builder number: " + builderNumber);
                List<Serializable> builderParams = new ArrayList<>();
                for (final BaseOperation baseOperation : transaction.getOperations()) {
                    builderParams.add(builderNumber);
                    builderParams.add(baseOperation);
                    ApiCall addOperationToBuilderTransaction = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(),
                            "add_operation_to_builder_transaction", builderParams,
                            RPC.VERSION, currentId);
                    websocket.sendText(addOperationToBuilderTransaction.toJsonString());
                }
            }
            if (!offset && currentId == OPERATION_ADDITION_RESPONSE || offset && currentId == OPERATION_ADDITION_RESPONSE + transaction.getOperations().size() - 1) {
                if (Configuration.SET_FEES_BEFORE_BROADCAST) {
                    List<Serializable> builderParams = new ArrayList<>();
                    builderParams.add(builderNumber);
                    builderParams.add(Configuration.ADDRESS_PREFIX);
                    ApiCall getRequiredFees = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(),
                            "set_fees_on_builder_transaction", builderParams,
                            RPC.VERSION, currentId);
                    websocket.sendText(getRequiredFees.toJsonString());
                } else {
                    currentId = FEE_RESPONSE + (offset ? transaction.getOperations().size() - 1 : 0);
                }
            }
            if (!offset && currentId == FEE_RESPONSE || offset && currentId == FEE_RESPONSE + transaction.getOperations().size() - 1) {
                List<Serializable> builderParams = new ArrayList<>();
                builderParams.add(builderNumber);
                builderParams.add(Configuration.TRANSACTION_BUILDER_SIGN_WITH_INSTANT_BROADCAST);
                ApiCall signBuilderTransaction = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(),
                        "sign_builder_transaction", builderParams, RPC.VERSION,
                        currentId);
                websocket.sendText(signBuilderTransaction.toJsonString());

                if (Configuration.SEND_WRITE_ASYNC && Configuration.TRANSACTION_BUILDER_SIGN_WITH_INSTANT_BROADCAST) {
                    LOG.info("Sent async - with instant broadcast");
                    writeStatisticObject.setEndTime(-1);
                    responseFuture.complete(ImmutablePair.of(false, "Sent async"));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }

            }
            if (!offset && currentId == SIGN_RESPONSE || offset && currentId == SIGN_RESPONSE + transaction.getOperations().size() - 1) {
                List<Serializable> builderParams = new ArrayList<>();
                if (!Configuration.TRANSACTION_BUILDER_SIGN_WITH_INSTANT_BROADCAST) {
                    String jsonString = frame.getPayloadText();
                    JSONObject jsonObject = new JSONObject(jsonString);
                    JSONObject signedTransaction = jsonObject.getJSONObject("result");
                    builderParams.add(new JsonSerializer(signedTransaction.toString()));

                    builderParams.add(Configuration.TRANSACTION_BUILDER_SIGN_BROADCAST);
                    ApiCall broadcastTransaction = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(),
                            "broadcast_transaction"
                            , builderParams, RPC.VERSION,
                            currentId);
                    websocket.sendText(broadcastTransaction.toJsonString());

                    if (Configuration.SEND_WRITE_ASYNC) {
                        LOG.info("Sent async - without instant broadcast");
                        writeStatisticObject.setEndTime(-1);
                        responseFuture.complete(ImmutablePair.of(false, "Sent async"));
                        if (oneTime) {
                            websocket.disconnect();
                        }
                        return;
                    }

                } else {
                    writeStatisticObject.setEndTime(System.nanoTime());
                    LOG.info("Not broadcasting, already done");
                    responseFuture.complete(ImmutablePair.of(false, frame.getPayloadText()));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                }
            }
            if (!offset && currentId == SIGN_RESPONSE || offset && currentId == SIGN_RESPONSE + transaction.getOperations().size() - 1) {
                writeStatisticObject.setEndTime(System.nanoTime());
                LOG.info("Final result: " + frame.getPayloadText());
                responseFuture.complete(ImmutablePair.of(false, frame.getPayloadText()));
                if (oneTime) {
                    websocket.disconnect();
                }
            }
            if (!offset && currentId > SIGN_RESPONSE || offset && currentId > SIGN_RESPONSE + transaction.getOperations().size() - 1) {
                LOG.info("Out of expected message range - not aborting: " + frame.getPayloadText());
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
            LOG.info("Frame sent: " + frame.getPayloadText());
        }
    }

    @Suspendable
    @Override
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("Error while broadcasting, cause: " + cause.getMessage());
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
        responseFuture.complete(ImmutablePair.of(true, cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

    @Suspendable
    @Override
    public void handleCallbackError(final WebSocket websocket, final Throwable cause) {
        LOG.error("Error in handleCallbackError while broadcasting, cause: " + cause.getMessage() + ", error: " + cause.getClass());
        for (final StackTraceElement element : cause.getStackTrace()) {
            LOG.error(element.getFileName() + "#" + element.getClassName() + ":" + element.getLineNumber());
        }
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
        responseFuture.complete(ImmutablePair.of(true, cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }
    }

    private static class JsonSerializer implements JsonSerializable {

        private final String signedTransaction;

        JsonSerializer(final String signedTransactionConstructor) {
            signedTransaction = signedTransactionConstructor;
        }

        @Suspendable
        @Override
        public String toJsonString() {
            return signedTransaction;
        }

        @Suspendable
        @Override
        public JsonElement toJsonObject() {

            JsonParser jsonParser = new JsonParser();
            return jsonParser.parse(new JSONObject(signedTransaction).toString()).getAsJsonObject();
        }

    }

}