package graphene.listener;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IListenerLogic;
import client.statistics.IStatistics;
import client.statistics.ListenerReferenceValues;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import com.neovisionaries.ws.client.WebSocketListener;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.api.BaseGrapheneHandler;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.ApiCall;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.DynamicGlobalProperties;
import graphene.components.BlockNumData;
import graphene.configuration.ApiId;
import graphene.configuration.Configuration;
import graphene.connection.GrapheneWebsocket;
import graphene.statistics.ListenerStatisticObject;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import sawtooth.statistics.BlockStatisticObject;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GrapheneSubscription extends BaseGrapheneHandler implements IListenerDisconnectionLogic, IListenerLogic {

    private static final Logger LOG = Logger.getLogger(GrapheneSubscription.class);

    private static final int LOGIN_ID = 2;
    private static final int DATABASE_ID = 3;
    private static final int SUBSCRIPTION_CALLBACK_ID = 4;
    private static final int PENDING_TRANSACTION_CALLBACK_ID = 5;
    private static final int BLOCK_APPLIED_CALLBACK_ID = 6;
    private static final int AUTO_SUBSCRIPTION_ID = 7;
    private static final List<Serializable> EMPTY_PARAMS = new ArrayList<>();
    private static final Map<String, Map<String, MutablePair<Long, Long>>> OBTAINED_EVENTS_MAP =
            new ConcurrentHashMap<>();
    private static final AtomicBoolean STATS_RETRIEVED = new AtomicBoolean(false);
    private static final AtomicInteger RECEIVED_COUNTER = new AtomicInteger(0);
    private static final int DEFAULT_INVALID_VALUE = -1;
    private static final long DEFAULT_EXISTING_VALUE = -3;
    private static final Map<Integer, Map<Integer, Integer>> BLOCK_STATISTIC_MAP = Collections.synchronizedMap(new LinkedHashMap<>());
    private final WitnessResponseListener listener;
    private final boolean oneTime;
    private final Queue<ClientObject> clientObjectList;
    private final Map<Integer, Integer> txMap = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> opMap = new ConcurrentHashMap<>();
    private final int numberOfExpectedEvents;
    private final CompletableFuture<Boolean> done = new CompletableFuture<>();
    private final CompletableFuture<Boolean> isSubscribed = new CompletableFuture<>();
    private final String regex;
    private final Queue<IStatistics> iStatistics;
    private final int totalNumberOfExpectedEventsPerClient;
    private final double threshold;
    private final double totalThreshold;
    private final AtomicBoolean statsSet = new AtomicBoolean(false);
    private int currentId = 1;
    private boolean isSubscriptionDone = false;
    private int currentNumberOfEventsPerClient = 0;
    private final int walletOffset;

    public GrapheneSubscription(final boolean oneTimeConstructor, final WitnessResponseListener listenerConstructor,
                                final Queue<ClientObject> clientObjectListConstructor,
                                final int numberOfExpectedEventsPerClientConstructor,
                                final int totalNumberOfExpectedEventsPerClientConstructor,
                                final String regexConstructor,
                                final double thresholdConstructor,
                                final double totalThresholdConstructor,
                                final Queue<IStatistics> iStatisticsConstructor,
                                final int walletOffsetConstructor) {
        super(listenerConstructor);
        this.oneTime = oneTimeConstructor;
        this.listener = listenerConstructor;
        this.clientObjectList = clientObjectListConstructor;
        this.numberOfExpectedEvents = numberOfExpectedEventsPerClientConstructor;
        this.totalNumberOfExpectedEventsPerClient = totalNumberOfExpectedEventsPerClientConstructor;
        this.regex = regexConstructor;
        this.threshold = thresholdConstructor;
        this.iStatistics = iStatisticsConstructor;
        this.totalThreshold = totalThresholdConstructor;
        this.walletOffset = walletOffsetConstructor;
    }

    @Suspendable
    public static Map<String, Map<String, MutablePair<Long, Long>>> getObtainedEventsMap() {
        return OBTAINED_EVENTS_MAP;
    }

    @Suspendable
    public CompletableFuture<Boolean> getIsSubscribed() {
        return isSubscribed;
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
            LOG.trace("Subscription received: " + frame.getPayloadText());
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

            if (isSubscriptionDone) {

                String webSocketAddress =
                        websocket.getURI().getScheme() + "://" + websocket.getURI().getHost() + ":" + (websocket.getURI().getPort() + walletOffset);
                if(Configuration.ENABLE_BLOCK_STATISTICS) {
                    getBlockNum(frame.getPayloadText(), webSocketAddress);
                }

                //createSumOfTxPerBlock(frame);
                //createSumOfOpPerBlock(frame);
                handleEvent(frame, clientObjectList);
            } else {
                if (currentId == LOGIN_ID) {
                    ApiCall database = new ApiCall(ApiId.ACCESS_RESTRICTED_API.ordinal(), "database", EMPTY_PARAMS,
                            RPC.VERSION, currentId);
                    websocket.sendText(database.toJsonString());
                }
                if (currentId == DATABASE_ID) {
                    if (Configuration.SET_SUBSCRIBE_CALLBACK) {
                        List<Serializable> params = new ArrayList<>();
                        params.add(Configuration.SUBSCRIBE_CALLBACK_ID);
                        params.add(Configuration.NOTIFY_REMOVE_CREATE);
                        ApiCall subscriptionCallback = new ApiCall(ApiId.FOLLOW_UP_RESTRICTED_API.ordinal(),
                                "set_subscribe_callback", params, RPC.VERSION,
                                currentId);

                        websocket.sendText(subscriptionCallback.toJsonString());
                    } else {
                        LOG.debug("ID set to 4");
                        currentId = SUBSCRIPTION_CALLBACK_ID;
                    }
                }
                if (currentId == SUBSCRIPTION_CALLBACK_ID) {
                    if (Configuration.SET_PENDING_TRANSACTION_CALLBACK) {
                        List<Serializable> params = new ArrayList<>();
                        params.add(Configuration.PENDING_TRANSACTION_CALLBACK_ID);
                        ApiCall pendingTransactionCallback = new ApiCall(ApiId.FOLLOW_UP_RESTRICTED_API.ordinal(),
                                "set_pending_transaction_callback", params,
                                RPC.VERSION, currentId);
                        websocket.sendText(pendingTransactionCallback.toJsonString());
                    } else {
                        LOG.debug("ID set to 5");
                        currentId = PENDING_TRANSACTION_CALLBACK_ID;
                    }
                }
                if (currentId == PENDING_TRANSACTION_CALLBACK_ID) {
                    if (Configuration.SET_BLOCK_APPLIED_CALLBACK) {
                        List<Serializable> params = new ArrayList<>();
                        params.add(Configuration.BLOCK_APPLIED_CALLBACK_ID);
                        ApiCall subscription = new ApiCall(ApiId.FOLLOW_UP_RESTRICTED_API.ordinal(),
                                "set_block_applied_callback", params, RPC.VERSION, currentId);
                        websocket.sendText(subscription.toJsonString());
                    } else {
                        LOG.debug("ID set to 6");
                        currentId = BLOCK_APPLIED_CALLBACK_ID;
                    }
                }
                if (currentId == BLOCK_APPLIED_CALLBACK_ID) {
                    if (Configuration.SET_AUTO_SUBSCRIPTION) {
                        List<Serializable> params = new ArrayList<>();
                        params.add(Configuration.ENABLE_AUTO_SUBSCRIPTION);
                        ApiCall subscription = new ApiCall(ApiId.FOLLOW_UP_RESTRICTED_API.ordinal(),
                                "set_auto_subscription",
                                params, RPC.VERSION, currentId);
                        websocket.sendText(subscription.toJsonString());
                    } else {
                        LOG.debug("ID set to 7");
                        currentId = AUTO_SUBSCRIPTION_ID;
                    }
                }
                if (currentId == AUTO_SUBSCRIPTION_ID) {
                    isSubscriptionDone = true;
                    LOG.info("Subscription done");
                    isSubscribed.complete(true);
                }
            }

        }
    }

    @Suspendable
    private void handleEvent(final WebSocketFrame frame, final Queue<ClientObject> clientObjectList) {

        if (Configuration.HANDLE_EVENT_SYNCHRONIZED) {
            synchronized (OBTAINED_EVENTS_MAP) {
                handleEventLogic(frame, clientObjectList);
            }
        } else {
            handleEventLogic(frame, clientObjectList);
        }
    }

    @Suspendable
    private void handleEventLogic(final WebSocketFrame frame, final Queue<ClientObject> clientObjectQueue) {
        if (!statsSet.get()) {

            if (done.isDone()) {
                return;
            }

            clientObjectQueue.parallelStream().forEach(clientObject -> {
            //for (final ClientObject clientObject : clientObjectQueue) {
                String id = clientObject.getClientId();
                if (OBTAINED_EVENTS_MAP.get(id) == null) {
                    LOG.debug("Unknown map entry: " + id);
                } else {

                    // ToDo maybe resend the transaction if it is not in the received block (expected block known)
                    Matcher matcher = checkEvent(frame.getPayloadText(), regex);
                    while (matcher.find()) {
                        if ("".equals(matcher.group(1))) {
                            LOG.error("\"\".equals(matcher.group(1)");
                            continue;
                        }
                        String expectedValue = matcher.group(1);

                        if (OBTAINED_EVENTS_MAP.get(id).get(expectedValue) != null) {
                            MutablePair<Long, Long> longLongMutablePair =
                                    OBTAINED_EVENTS_MAP.get(id).get(expectedValue);
                            if (longLongMutablePair.getRight() != DEFAULT_INVALID_VALUE) {
                                LOG.error("Updating already existing value " + expectedValue + " - possible " +
                                        "duplicate event received");
                                if (Configuration.RETURN_ON_EVENT_DUPLICATE) {
                                    LOG.error("Returned due to duplicated");
                                    continue;
                                }
                            }
                            longLongMutablePair.setRight(System.nanoTime());
                            ListenerReferenceValues.getTimeMap().get(id).get(expectedValue).setRight(System.currentTimeMillis());

                            OBTAINED_EVENTS_MAP.get(id)
                                    .replace(
                                            expectedValue,
                                            longLongMutablePair);

                            LOG.debug(id + " received expected value: " + expectedValue);

                            ++currentNumberOfEventsPerClient;
                            int receivedCounter = RECEIVED_COUNTER.incrementAndGet();
                            if (checkThreshold(threshold, currentNumberOfEventsPerClient, numberOfExpectedEvents,
                                    false)) {
                                if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter,
                                        totalNumberOfExpectedEventsPerClient, true)) {
                                    synchronized (this) {
                                        if(!statsSet.get()) {
                                            ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                            listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                            listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                            listenerStatisticObject.setExpectedThreshold(threshold);
                                            listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                            iStatistics.add(listenerStatisticObject);
                                            statsSet.set(true);
                                        }
                                    }

                                }
                                done.complete(true);
                                return;
                            }
                        } else {
                            LOG.debug("Received event value: " + expectedValue + " not contained for key: " + id);
                        }

                        //for (final String event : Configuration.EVENT_EXISTS_SUFFIX_LIST) {
                        Configuration.EVENT_EXISTS_SUFFIX_LIST.parallelStream().forEach(event -> {
                            if (!expectedValue.endsWith(event)) {
                                LOG.debug("Not checking for existing event");
                            } else {
                                if (OBTAINED_EVENTS_MAP.get(id).get(expectedValue.replace(event, "")) != null) {
                                    MutablePair<Long, Long> longLongMutablePair =
                                            OBTAINED_EVENTS_MAP.get(id).get(expectedValue.replace(event, ""));
                                    if (longLongMutablePair.getRight() != DEFAULT_INVALID_VALUE) {
                                        LOG.error("Updating already existing value " + expectedValue.replace(event,
                                                "") + " - possible " +
                                                "duplicate event received (existing)");
                                        if (Configuration.RETURN_ON_EVENT_DUPLICATE) {
                                            LOG.error("Returned due to duplicated (existing)");
                                            return;
                                            //continue;
                                        }
                                    }
                                    longLongMutablePair.setRight(DEFAULT_EXISTING_VALUE);
                                    OBTAINED_EVENTS_MAP.get(id)
                                            .replace(
                                                    expectedValue.replace(event, ""),
                                                    longLongMutablePair);

                                    LOG.debug(id + " received expected value (existing): " + expectedValue.replace(
                                            event, ""));

                                    ++currentNumberOfEventsPerClient;
                                    int receivedCounter = RECEIVED_COUNTER.incrementAndGet();
                                    if (checkThreshold(threshold, currentNumberOfEventsPerClient,
                                            numberOfExpectedEvents,
                                            false)) {
                                        if (!statsSet.get() && checkThreshold(totalThreshold, receivedCounter,
                                                totalNumberOfExpectedEventsPerClient, true)) {
                                            synchronized (this) {
                                                if(!statsSet.get()) {
                                                    ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
                                                    listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
                                                    listenerStatisticObject.setSetThreshold(((double) receivedCounter / numberOfExpectedEvents));
                                                    listenerStatisticObject.setExpectedThreshold(threshold);
                                                    listenerStatisticObject.setSetTotalThreshold(totalThreshold);
                                                    iStatistics.add(listenerStatisticObject);
                                                    statsSet.set(true);
                                                }
                                            }
                                        }
                                        done.complete(true);
                                        return;
                                    }
                                }
                            }
                        });
                    }
                }
            });
        }
    }

    @Suspendable
    private static Matcher checkEvent(final String event, final String regex) {
        LOG.debug("Using regex: " + regex);
        Pattern pattern = Pattern.compile(regex,
                Pattern.DOTALL);
        return pattern.matcher(event);
    }

    @Suspendable
    private boolean checkThreshold(final double threshold, final int currentNumberOfEvents,
                                   final int numberOfExpectedEvents, final boolean isTotal) {
        if (threshold <= ((double) currentNumberOfEvents / numberOfExpectedEvents)) {
            LOG.info("Reached threshold of " + threshold + " aborting with value: " + ((double) currentNumberOfEvents / numberOfExpectedEvents) + " current number of events: " + (double) currentNumberOfEvents + " number of expected events " + numberOfExpectedEvents
                    + " is total: " + isTotal);
            return true;
        }
        return false;
    }

    @Suspendable
    private void handleFurtherStatistics(final WebSocketFrame frame) {
        try {
            JSONObject jsonObject = new JSONObject(frame.getPayloadText());
            JSONArray jsonArray = jsonObject.getJSONArray("params");
            JSONArray jsonArrayBlock = jsonArray.getJSONArray(1);

            for (int i = 0; i < jsonArrayBlock.length(); i++) {

                BlockStructure[] blockStructureArr =
                        new Gson().fromJson(String.valueOf(jsonArrayBlock.getJSONArray(i)),
                                BlockStructure[].class);

                for (final BlockStructure blockStructure : blockStructureArr) {

                    if (blockStructure.getTrxInBlock() != null) {

                        if (blockStructure.getBlockNum() != null && blockStructure.getTrxInBlock() != null && blockStructure.getOpInTrx() != null) {

                            synchronized (BLOCK_STATISTIC_MAP) {
                                if (!BLOCK_STATISTIC_MAP.containsKey(blockStructure.getBlockNum())) {
                                    BLOCK_STATISTIC_MAP.put(blockStructure.getBlockNum(), Collections.synchronizedMap(new LinkedHashMap<>()));
                                    BLOCK_STATISTIC_MAP.get(blockStructure.getBlockNum()).put(blockStructure.getTrxInBlock() + 1, blockStructure.getOpInTrx() + 1);
                                    LOG.info("Put block id: " + blockStructure.getBlockNum() + " trx in block  " + blockStructure.getTrxInBlock() + " operations in trx " + blockStructure.getOpInTrx());
                                } else {
                                    if (BLOCK_STATISTIC_MAP.get(blockStructure.getBlockNum()).containsKey(blockStructure.getTrxInBlock() + 1)) {
                                        if (BLOCK_STATISTIC_MAP.get(blockStructure.getBlockNum()).get(blockStructure.getTrxInBlock() + 1) < blockStructure.getOpInTrx() + 1) {
                                            BLOCK_STATISTIC_MAP.get(blockStructure.getBlockNum()).put(blockStructure.getTrxInBlock() + 1, blockStructure.getOpInTrx() + 1);

                                            LOG.info("Updated block id: " + blockStructure.getBlockNum() + " with a " +
                                                    "new maximum of " + blockStructure.getOpInTrx() + " operations");

                                        } else {
                                            LOG.info("Discarding smaller value " + (blockStructure.getOpInTrx() + 1) + " current value: " + BLOCK_STATISTIC_MAP.get(blockStructure.getBlockNum()).get(blockStructure.getTrxInBlock() + 1));
                                        }
                                    } else {
                                        BLOCK_STATISTIC_MAP.get(blockStructure.getBlockNum()).put(blockStructure.getTrxInBlock() + 1, blockStructure.getOpInTrx() + 1);
                                        LOG.info("Put transaction in block: " + (blockStructure.getTrxInBlock() + 1) + " operation in block " + (blockStructure.getOpInTrx() + 1));
                                    }
                                }
                            }

                        }

                    }
                }
            }

            for (Map.Entry<Integer, Map<Integer, Integer>> integerMapEntry : BLOCK_STATISTIC_MAP.entrySet()) {
                for (Map.Entry<Integer, Integer> integerIntegerEntry : integerMapEntry.getValue().entrySet()) {
                    LOG.info("Block number: " + integerMapEntry.getKey() + " transaction number " + integerIntegerEntry.getKey() + " operation number " + integerIntegerEntry.getValue());
                }
            }

        } catch (JSONException | JsonSyntaxException ex) {
            LOG.error("JSON invalid, continuing " + ex.getMessage());
        }
    }

    @Suspendable
    private void createSumOfTxPerBlock(final WebSocketFrame frame) {
        Pattern pattern = Pattern.compile("\"block_num\":(.*?),\"trx_in_block\":(.*?),\"", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(frame.getPayloadText());
        while (matcher.find()) {
            if (matcher.group(1).matches("-?(0|[1-9]\\d*)")) {
                synchronized (txMap) {
                    if (txMap.containsKey(Integer.parseInt(matcher.group(1)))) {
                        int max = txMap.get(Integer.parseInt(matcher.group(1)));
                        if (Integer.parseInt(matcher.group(2)) > max) {
                            max = Integer.parseInt(matcher.group(2)) + 1;
                            txMap.put(Integer.parseInt(matcher.group(1)), max);
                            LOG.info("Updated block id: " + Integer.parseInt(matcher.group(1)) + " with a new maximum" +
                                    " of "
                                    + max + " transactions");
                        }
                    } else {
                        txMap.put(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)) + 1);
                        LOG.info("Added block id: " + Integer.parseInt(matcher.group(1)) + " with " + (Integer.parseInt(matcher.group(2)) + 1) + " transactions");

                    }

                }
            } else {
                LOG.error("Not an integer parsed in " + matcher.toString() + " - tx calculation " + frame.getPayloadText());
            }
        }
    }

    @Suspendable
    private void createSumOfOpPerBlock(final WebSocketFrame frame) {
        Pattern pattern = Pattern.compile("\"block_num\":(.*?),\"trx_in_block\":.*?,\"op_in_trx\":(.*?),\"",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(frame.getPayloadText());
        while (matcher.find()) {
            if (matcher.group(1).matches("-?(0|[1-9]\\d*)")) {
                synchronized (opMap) {
                    if (opMap.containsKey(Integer.parseInt(matcher.group(1)))) {
                        int max = opMap.get(Integer.parseInt(matcher.group(1)));
                        if (Integer.parseInt(matcher.group(2)) > max) {
                            max = Integer.parseInt(matcher.group(2)) + 1;
                            opMap.put(Integer.parseInt(matcher.group(1)), max);
                            LOG.info("Updated block id: " + Integer.parseInt(matcher.group(1)) + " with a new maximum" +
                                    " of "
                                    + max + " operations");
                        }
                    } else {
                        opMap.put(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)) + 1);
                        LOG.info("Added block id: " + Integer.parseInt(matcher.group(1)) + " with " + (Integer.parseInt(matcher.group(2)) + 1) + " operations");
                    }
                }
            } else {
                LOG.error("Not an integer parsed in " + matcher.toString() + " - operation calculation " + frame.getPayloadText());
            }
        }
    }

    @Suspendable
    @Override
    public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
        if (frame.isTextFrame()) {
            LOG.info("Subscription sent: " + frame.getPayloadText());
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

        LOG.error("Subscription handleCallbackError, cause: " + cause.getMessage() + ", error: " + cause.getClass());
        for (final StackTraceElement element : cause.getStackTrace()) {
            LOG.error(element.getFileName() + "#" + element.getClassName() + ":" + element.getLineNumber());
        }
        listener.onError(new BaseResponse.Error(cause.getMessage()));
        if (oneTime) {
            websocket.disconnect();
        }

    }

    @Suspendable
    @Override
    public CompletableFuture<Boolean> isDone() {
        return done;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public synchronized final <E> Queue<IStatistics> getStatistics(final E... params) {
        if (statsSet.get() && !STATS_RETRIEVED.get()) {
            STATS_RETRIEVED.set(true);
            return iStatistics;
        } else {
            return new ConcurrentLinkedQueue<>();
        }
    }

    @Override
    @Suspendable
    public synchronized void setStatisticsAfterTimeout() {
        if(!statsSet.get()) {
            ListenerStatisticObject listenerStatisticObject = new ListenerStatisticObject();
            listenerStatisticObject.setObtainedEventsMap(OBTAINED_EVENTS_MAP);
            listenerStatisticObject.setSetThreshold(-1);
            listenerStatisticObject.setExpectedThreshold(threshold);
            listenerStatisticObject.setSetTotalThreshold(totalThreshold);
            iStatistics.add(listenerStatisticObject);
            statsSet.set(true);
        }
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleEvent(final E... params) {
        WebSocketFrame frame = (WebSocketFrame) params[0];
        Queue<ClientObject> clientObjectList = (Queue<ClientObject>) params[1];
        handleEvent(frame, clientObjectList);
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> boolean checkThreshold(final E... params) {
        double threshold = (Double) params[0];
        int currentNumberOfEvents = (Integer) params[1];
        int numberOfExpectedEvents = (Integer) params[2];
        boolean isTotal = (Boolean) params[3];
        return checkThreshold(threshold, currentNumberOfEvents, numberOfExpectedEvents, isTotal);
    }

    private final List<Integer> handledBlockList = new ArrayList<>();

    @Suspendable
    public void getBlockNum(final String subscriptionString, final String webSocketAddress) {
        Pattern pattern = Pattern.compile("\"block_num\":(.*?),\"", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(subscriptionString);
        while (matcher.find()) {
            if (matcher.group(1).matches("\\d+") && !handledBlockList.contains(Integer.parseInt(matcher.group(1)))) {
                handledBlockList.add(Integer.valueOf(matcher.group(1)));
                getBlockData(Integer.parseInt(matcher.group(1)), webSocketAddress);
            } else {
                LOG.debug("Not an integer parsed in " + matcher.toString() + " - " + subscriptionString);
            }
        }
    }

    @Suspendable
    public void getBlockData(final int blockNum, final String webSocketAddress) {

                    /*List<String> servers = GenericSelectionStrategy.selectFixed(GrapheneHelper.getAccounts(true),
                Collections.singletonList(0), false);
        WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                servers.get(0));*/

            WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                    webSocketAddress);

            BlockNumData blockNumData = new BlockNumData(blockNum, true,
                    new GrapheneWitnessListener().registerWitnessResponseListener());

            List<WebSocketListener> webSocketListenerList = new ArrayList<>();
            webSocketListenerList.add(blockNumData);

            GrapheneWebsocket.connectToServer(webSocket, webSocketListenerList);

        try {
            BlockWrapperWallet blockWrapperWallet = blockNumData.getBlockWrapperWalletCompletableFuture().get(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);

            if(blockWrapperWallet.getResult() != null) {
                BlockStatisticObject blockStatisticObject = new BlockStatisticObject();
                blockStatisticObject.setBlockId(blockWrapperWallet.getResult().getBlockId());
                blockStatisticObject.setReceivedTime(System.currentTimeMillis());
                blockStatisticObject.setClientId(clientObjectList.stream().map(ClientObject::getClientId).collect(Collectors.toList()).toString());
                blockStatisticObject.setNumberOfTransactions(blockWrapperWallet.getResult().getTransactions().size());
                blockStatisticObject.setNumberOfActions(blockWrapperWallet.getResult().getTransactions().stream().mapToInt(transaction -> transaction.getOperations().size()).sum());
                blockStatisticObject.setBlockNum(blockNum);
                blockWrapperWallet.getResult().getTransactionIds().forEach(transactionId -> blockStatisticObject.getTxIdList().add(transactionId));
                iStatistics.add(blockStatisticObject);
            }
            else {
                LOG.error("No block data for number obtained");
            }

        } catch (InterruptedException | ExecutionException ex) {
            ExceptionHandler.logException(ex);
        } catch (TimeoutException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
