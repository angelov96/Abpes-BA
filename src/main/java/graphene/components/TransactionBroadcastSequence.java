package graphene.components;

import client.configuration.GeneralConfiguration;
import co.paralleluniverse.fibers.Suspendable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import cy.agorise.graphenej.*;
import cy.agorise.graphenej.api.BaseGrapheneHandler;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.ApiCall;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.DynamicGlobalProperties;
import cy.agorise.graphenej.models.WitnessResponse;
import graphene.configuration.ApiId;
import graphene.configuration.Configuration;
import graphene.helper.Helper;
import graphene.statistics.WriteStatisticObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TransactionBroadcastSequence extends BaseGrapheneHandler {

    private static final int LOGIN_ID = 2;
    private static final int GET_NETWORK_BROADCAST_ID = 3;
    private static final int GET_NETWORK_DYNAMIC_PARAMETERS = 4;
    private static final int GET_REQUIRED_FEES = 5;
    private static final int BROADCAST_TRANSACTION = 6;

    private static final Logger LOG = Logger.getLogger(TransactionBroadcastSequence.class);

    private final Asset feeAsset;
    private final graphene.components.Transaction transaction;
    private final WitnessResponseListener listener;
    private final boolean oneTime;
    private final CompletableFuture<ImmutablePair<Boolean, String>> responseFuture = new CompletableFuture<>();
    private final WriteStatisticObject writeStatisticObject;
    private final int walletOffset;
    private int currentId = 1;
    private int broadcastApiId = -1;

    public TransactionBroadcastSequence(final graphene.components.Transaction transaction, final Asset feeAsset,
                                        final WitnessResponseListener listener,
                                        final WriteStatisticObject writeStatisticObjectConstructor
            , final int walletOffsetConstructor) {
        this(transaction, feeAsset, true, listener, writeStatisticObjectConstructor, walletOffsetConstructor);
    }

    public TransactionBroadcastSequence(final graphene.components.Transaction transactionConstructor,
                                        final Asset feeAssetConstructor, final boolean oneTimeConstructor
            , final WitnessResponseListener listenerConstructor,
                                        final WriteStatisticObject writeStatisticObjectConstructor,
                                        final int walletOffsetConstructor) {
        super(listenerConstructor);
        this.transaction = transactionConstructor;
        this.feeAsset = feeAssetConstructor;
        this.oneTime = oneTimeConstructor;
        this.listener = listenerConstructor;
        this.writeStatisticObject = writeStatisticObjectConstructor;
        this.walletOffset = walletOffsetConstructor;
    }

    @Suspendable
    public CompletableFuture<ImmutablePair<Boolean, String>> getResponseFuture() {
        return responseFuture;
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
    public void onTextFrame(WebSocket websocket, WebSocketFrame frame) throws Exception {

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
            LOG.info("Received text: " + frame.getPayloadText());
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
            ArrayList<Serializable> emptyParams = new ArrayList<>();
            if (currentId == LOGIN_ID) {
                ApiCall networkApiIdCall = new ApiCall(ApiId.ACCESS_RESTRICTED_API.ordinal(),
                        RPC.CALL_NETWORK_BROADCAST, emptyParams, RPC.VERSION,
                        currentId);
                websocket.sendText(networkApiIdCall.toJsonString());
            }
            if (currentId == GET_NETWORK_BROADCAST_ID) {
                Type ApiIdResponse = new TypeToken<WitnessResponse<Integer>>() {
                }.getType();
                WitnessResponse<Integer> witnessResponse = gson.fromJson(response, ApiIdResponse);
                broadcastApiId = witnessResponse.result;

                ApiCall getDynamicParametersCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(),
                        RPC.CALL_GET_DYNAMIC_GLOBAL_PROPERTIES,
                        emptyParams,
                        RPC.VERSION,
                        currentId);

                websocket.sendText(getDynamicParametersCall.toJsonString());

            }
            if (currentId == GET_NETWORK_DYNAMIC_PARAMETERS) {
                Type DynamicGlobalPropertiesResponse = new TypeToken<WitnessResponse<DynamicGlobalProperties>>() {
                }.getType();
                WitnessResponse<DynamicGlobalProperties> witnessResponse = gson.fromJson(response,
                        DynamicGlobalPropertiesResponse);
                DynamicGlobalProperties dynamicProperties = witnessResponse.result;

                long expirationTime = (dynamicProperties.time.getTime() / 1000) + Transaction.DEFAULT_EXPIRATION_TIME;
                String headBlockId = dynamicProperties.head_block_id;
                long headBlockNumber = dynamicProperties.head_block_number;
                transaction.setBlockData(new BlockData(headBlockNumber, headBlockId,
                        expirationTime));
                LOG.debug("Expiration time: " + expirationTime + " HeadBlockId " + headBlockId + " BlockNumber: " + headBlockNumber);

                if (Configuration.SET_FEES_BEFORE_BROADCAST) {
                    ArrayList<Serializable> feeParams = new ArrayList<>();
                    feeParams.add((Serializable) transaction.getOperations());
                    feeParams.add(this.feeAsset.getObjectId());
                    ApiCall getRequiredFees = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(),
                            RPC.CALL_GET_REQUIRED_FEES,
                            feeParams, RPC.VERSION,
                            currentId);

                    websocket.sendText(getRequiredFees.toJsonString());
                } else {
                    currentId = GET_REQUIRED_FEES;
                }
            }
            if (currentId == GET_REQUIRED_FEES) {
                if (Configuration.SET_FEES_BEFORE_BROADCAST) {
                    Type GetRequiredFeesResponse = new TypeToken<WitnessResponse<List<AssetAmount>>>() {
                    }.getType();
                    GsonBuilder gsonBuilder = new GsonBuilder();
                    gsonBuilder.registerTypeAdapter(AssetAmount.class, new AssetAmount.AssetAmountDeserializer());
                    WitnessResponse<List<AssetAmount>> requiredFeesResponse = gsonBuilder.create().fromJson(response,
                            GetRequiredFeesResponse);

                    transaction.setFees(requiredFeesResponse.result);
                }

                byte[] signatureRemote = new byte[0];

                if (!Configuration.SIGN_LOCAL) {
                    String webSocketWalletAddress =
                            websocket.getURI().getScheme() + "://" + websocket.getURI().getHost() + ":" + (websocket.getURI().getPort() + walletOffset);
                    ImmutablePair<Boolean, String> signatureRemoteTemp = transaction.getSignatureRemote(transaction,
                            webSocketWalletAddress);
                    if (signatureRemoteTemp.getLeft()) {
                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        responseFuture.complete(signatureRemoteTemp);
                        return;
                    } else {
                        signatureRemote = signatureRemoteTemp.getRight().getBytes(StandardCharsets.UTF_8);
                    }

                    if (Configuration.SEND_WRITE_ASYNC && Configuration.INSTANT_BROADCAST_AFTER_SIGNING) {
                        LOG.info("Sent async - with instant broadcast");
                        writeStatisticObject.setEndTime(-1);
                        responseFuture.complete(ImmutablePair.of(false, "Sent async"));
                        if (oneTime) {
                            websocket.disconnect();
                        }
                        return;
                    }

                }

                if (Configuration.OBTAIN_TX_ID) {
                    if (!Configuration.OBTAIN_TX_ID_LOCAL) {
                        String webSocketWalletAddress =
                                websocket.getURI().getScheme() + "://" + websocket.getURI().getHost() + ":" + (websocket.getURI().getPort() + walletOffset);
                        ImmutablePair<Boolean, String> txId = transaction.getTxId(transaction, webSocketWalletAddress);
                        if (txId.getLeft()) {
                            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            responseFuture.complete(txId);
                            return;
                        }
                    } else {
                        Helper.getTxId(transaction);
                    }
                }

                if (!Configuration.SIGN_LOCAL && Configuration.INSTANT_BROADCAST_AFTER_SIGNING) {
                    String signature = Util.bytesToHex(signatureRemote);
                    LOG.info("Already handled while signed, response: " + signature);
                    writeStatisticObject.setEndTime(System.nanoTime());
                    responseFuture.complete(ImmutablePair.of(false, signature));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }

                ArrayList<Serializable> transactions = new ArrayList<>();
                transactions.add(transaction);

                ApiCall call = new ApiCall(broadcastApiId,
                        Configuration.BROADCAST_TYPE,
                        transactions,
                        RPC.VERSION,
                        currentId);
                websocket.sendText(call.toJsonString());

                if (Configuration.SEND_WRITE_ASYNC) {
                    LOG.info("Sent async - without instant broadcast");
                    writeStatisticObject.setEndTime(-1);
                    responseFuture.complete(ImmutablePair.of(false, "Sent async"));
                    if (oneTime) {
                        websocket.disconnect();
                    }
                    return;
                }

            }
            if (currentId == BROADCAST_TRANSACTION) {
                LOG.info("Final result: " + frame.getPayloadText());
                writeStatisticObject.setEndTime(System.nanoTime());
                responseFuture.complete(ImmutablePair.of(false, frame.getPayloadText()));
                if (oneTime) {
                    websocket.disconnect();
                }
            }
            if (currentId < LOGIN_ID || currentId > BROADCAST_TRANSACTION) {
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
            LOG.info("Broadcast sequence sent: " + frame.getPayloadText());
        }
    }

    @Suspendable
    @Override
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("onError, cause: " + cause.getMessage());
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
        LOG.error("handleCallbackError, cause: " + cause.getMessage() + ", error: " + cause.getClass());
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
}