package sawtooth.listener;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;
import okhttp3.*;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.json.JSONException;
import org.json.JSONObject;
import sawtooth.configuration.Configuration;
import sawtooth.endpointdata.singleblock.Data;
import sawtooth.endpointdata.singleblock.SingleBlock;
import sawtooth.rest.ISawtoothClientRest;
import sawtooth.rest.RetrofitHelper;
import sawtooth.statistics.BlockStatisticObject;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class WebsocketListener implements IListenerDisconnectionLogic {

    private static final Logger LOG = Logger.getLogger(WebsocketListener.class);

    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private final Queue<ClientObject> clientObjectQueue;

    @SafeVarargs
    @Override
    @Suspendable
    public synchronized final <E> Queue<IStatistics> getStatistics(final E... params) {
            return iStatistics;
    }

    @Override
    @Suspendable
    public void setStatisticsAfterTimeout() {

    }

    public WebsocketListener(final Queue<ClientObject> clientObjectQueueConstructor) {
        this.clientObjectQueue = clientObjectQueueConstructor;
    }

    public WebsocketListener() {
        clientObjectQueue = null;
    }

    private final AtomicBoolean isSubscribed = new AtomicBoolean(true);

    @Suspendable
    public WebSocket createWebsocketListener(final String urlToSubscribeTo, final boolean subscribe) {
        if(!subscribe) {
            isSubscribed.set(false);
        }
        Request request = new Request.Builder().url(urlToSubscribeTo).build();

        WebSocketListener webSocketListener = new WebSocketListener() {
            @Override
            @Suspendable
            public void onClosed(@NotNull final WebSocket webSocket, final int code, @NotNull final String reason) {
                super.onClosed(webSocket, code, reason);
                LOG.info("Websocket closed with reason: " + reason + " Code: " + code);
            }

            @Override
            @Suspendable
            public void onClosing(@NotNull final WebSocket webSocket, final int code, @NotNull final String reason) {
                super.onClosing(webSocket, code, reason);
                LOG.info("Websocket closing with reason: " + reason + " Code: " + code);
            }

            @Override
            @Suspendable
            public void onFailure(@NotNull final WebSocket webSocket, @NotNull final Throwable t,
                                  @Nullable final Response response) {
                super.onFailure(webSocket, t, response);
                LOG.info("Websocket failure with reason: " + Objects.requireNonNull(response).message());
            }

            @Override
            @Suspendable
            public void onMessage(@NotNull final WebSocket webSocket, @NotNull final String text) {
                super.onMessage(webSocket, text);

                if(Configuration.ENABLE_BLOCK_STATISTICS && !Configuration.USE_ZMQ_FOR_BLOCK_STATISTICS) {
                    if (isSubscribed.get()) {
                        JSONObject json = new JSONObject(text);
                        retrofit2.Retrofit retrofit = RetrofitHelper.buildRetrofit(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET[0]);
                        ISawtoothClientRest sawtoothClientRestInterface = retrofit.create(ISawtoothClientRest.class);
                        retrofit2.Call<SingleBlock> call = sawtoothClientRestInterface.getBlock(json.getString("block_id"));
                        call.enqueue(handleBlockStatistics());
                    }
                }

                LOG.info("Websocket receiving message with text: " + text);
            }

            @Override
            @Suspendable
            public void onMessage(@NotNull final WebSocket webSocket, @NotNull final okio.ByteString bytes) {
                super.onMessage(webSocket, bytes);
                LOG.info("Websocket receiving message with text (byte): " + bytes.toString());
            }

            @Override
            @Suspendable
            public void onOpen(@NotNull final WebSocket webSocket, final @NotNull Response response) {
                super.onOpen(webSocket, response);
                try {
                    LOG.info("Received response: " + response.message());
                    JSONObject jsonObject;
                    if (subscribe) {
                        for (final String json : Configuration.SUBSCRIPTION_JSON_STRINGS) {
                            jsonObject = new JSONObject(json);
                            webSocket.send(Objects.requireNonNull(jsonObject).toString());
                        }
                    } else {
                        for (final String json : Configuration.UNSUBSCRIPTION_JSON_STRINGS) {
                            jsonObject = new JSONObject(json);
                            webSocket.send(Objects.requireNonNull(jsonObject).toString());
                        }
                    }
                } catch (JSONException jsonException) {
                    LOG.error("JSON Error: " + jsonException.getMessage());
                }
            }
        };
        OkHttpClient client = new OkHttpClient.Builder()
                .build();
        return client.newWebSocket(request, webSocketListener);
    }

    @Suspendable
    @Override
    public CompletableFuture<Boolean> isDone() {
        CompletableFuture<Boolean> done = new CompletableFuture<>();
        done.complete(true);
        return done;
    }

    @Suspendable
    private retrofit2.Callback<SingleBlock> handleBlockStatistics() {
        return new retrofit2.Callback<SingleBlock>() {

            @Suspendable
            @Override
            public void onResponse(@NotNull final retrofit2.Call<SingleBlock> call,
                                   @NotNull final retrofit2.Response<SingleBlock> response) {
                if (response.body() != null) {
                    if (response.code() != 200) {
                        LOG.error("Unexpected response code: " + response.code() + " not 200");
                    } else {
                            Data blockData = response.body().getData();
                            int transactionCounter = blockData.getBatches().stream().mapToInt(batch -> batch.getTransactions().size()).sum();

                            BlockStatisticObject blockStatisticObject = new BlockStatisticObject();
                            blockStatisticObject.setBlockId(blockData.getHeaderSignature());
                            blockStatisticObject.setReceivedTime(System.currentTimeMillis());
                            blockStatisticObject.setClientId(clientObjectQueue.stream().map(ClientObject::getClientId).collect(Collectors.toList()).toString());
                            blockStatisticObject.setNumberOfTransactions(transactionCounter);
                            blockStatisticObject.setNumberOfActions(transactionCounter);
                            blockStatisticObject.setBlockNum(Long.parseLong(blockData.getHeader().getBlockNum()));
                            blockData.getBatches().forEach(batch -> batch.getTransactions().forEach(transaction -> blockStatisticObject.getTxIdList().add(transaction.getHeaderSignature())));
                            iStatistics.add(blockStatisticObject);
                        }
                    }
            }

            @Suspendable
            @Override
            public void onFailure(@NotNull final retrofit2.Call<SingleBlock> call, @NotNull final Throwable throwable) {
                LOG.error("Failure: " + throwable.getMessage());
            }
        };
    }

}
