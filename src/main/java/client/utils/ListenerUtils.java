package client.utils;

import client.client.ClientObject;
import client.commoninterfaces.IListenerDisconnectionLogic;
import client.commoninterfaces.IWorkloadObject;
import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.neovisionaries.ws.client.WebSocket;
import fabric.workloads.PrepareFabricWorkloadObject;
import graphene.workloads.PrepareGrapheneWorkloadObject;
import io.reactivex.disposables.Disposable;
import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import quorum.workloads.PrepareQuorumWorkloadObject;
import sawtooth.listener.WebsocketListener;
import sawtooth.listener.ZmqListener;
import sawtooth.workloads.PrepareSawtoothWorkloadObject;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ListenerUtils {

    private static final Logger LOG = Logger.getLogger(ListenerUtils.class);

    @Suspendable
    public static void awaitEndOfExecution(final IWorkloadObject prepareWorkloadObject,
                                           final Queue<IStatistics> statisticList) {
        switch (GeneralConfiguration.BLOCKCHAIN_FRAMEWORK) {
            case Corda:
                for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                        prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
                    try {
                        iListenerDisconnectionLogic.isDone().get(corda.configuration.Configuration.TIMEOUT_LISTENER,
                                corda.configuration.Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    } catch (InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        iListenerDisconnectionLogic.setStatisticsAfterTimeout();
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    }
                }
                return;
            case Quorum:
                for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                        prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
                    try {
                        iListenerDisconnectionLogic.isDone().get(quorum.configuration.Configuration.TIMEOUT_LISTENER,
                                quorum.configuration.Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    } catch (InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        iListenerDisconnectionLogic.setStatisticsAfterTimeout();
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    }
                }
                return;
            case HyperledgerSawtooth:
                for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                        prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
                    try {
                        iListenerDisconnectionLogic.isDone().get(sawtooth.configuration.Configuration.TIMEOUT_LISTENER,
                                sawtooth.configuration.Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    } catch (InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        iListenerDisconnectionLogic.setStatisticsAfterTimeout();
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    }
                }
                return;
            case HyperledgerFabric:
                for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                        prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
                    try {
                        iListenerDisconnectionLogic.isDone().get(fabric.configuration.Configuration.TIMEOUT_LISTENER,
                                fabric.configuration.Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    } catch (InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        iListenerDisconnectionLogic.setStatisticsAfterTimeout();
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    }
                }
                return;
            case Graphene:
                for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                        prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
                    try {
                        iListenerDisconnectionLogic.isDone().get(graphene.configuration.Configuration.TIMEOUT_LISTENER,
                                graphene.configuration.Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    } catch (InterruptedException | ExecutionException ex) {
                        ExceptionHandler.logException(ex);
                    } catch (TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                        iListenerDisconnectionLogic.setStatisticsAfterTimeout();
                        statisticList.addAll(iListenerDisconnectionLogic.getStatistics());
                    }
                }
                return;
            default:
                LOG.error("Returning awaitEndOfExecution due to default case");
        }
    }

    @Suspendable
    public static synchronized void disconnectListeners(final ClientObject clientObject,
                                                        final IWorkloadObject prepareWorkloadObject) {
        switch (GeneralConfiguration.BLOCKCHAIN_FRAMEWORK) {
            case Corda:
                return;
            case Quorum:
                if (quorum.configuration.Configuration.UNREGISTER_LISTENERS) {
                    for (final Disposable listener :
                            ((PrepareQuorumWorkloadObject) prepareWorkloadObject).getListener()) {
                        quorum.listener.Listener.unregisterListener(listener);
                    }
                    LOG.info("Closed listeners, finished " + clientObject.getClientId());
                }
                return;
            case HyperledgerSawtooth:
                if (sawtooth.configuration.Configuration.DISCONNECT_LISTENERS) {
                    for (final String webSocketSubscriptionServer :
                            ((PrepareSawtoothWorkloadObject) prepareWorkloadObject).getWebSocketSubscriptionServers()) {
                        WebsocketListener websocketListener =
                                new WebsocketListener();
                        websocketListener.createWebsocketListener(webSocketSubscriptionServer, false);
                        LOG.info("Closed websocket, finished " + clientObject.getClientId());
                    }
                    for (final Map.Entry<String, ZMQ.Socket> socketEntry :
                            ((PrepareSawtoothWorkloadObject) prepareWorkloadObject).getZmqSocketSubscriptionServerMap().entrySet()) {
                        ZmqListener.unsubscribeListener(socketEntry.getValue(), socketEntry.getKey());
                        LOG.info("Closed zmq listener, finished " + clientObject.getClientId());
                    }
                }
                return;
            case HyperledgerFabric:
                if (fabric.configuration.Configuration.UNREGISTER_LISTENERS) {
                    for (final Map.Entry<String, Boolean> listenerEntry :
                            ((PrepareFabricWorkloadObject) prepareWorkloadObject).getListener().entrySet()) {
                        fabric.listener.Listener.unregisterAndUnsetAll(listenerEntry.getKey(), listenerEntry.getValue(),
                                ((PrepareFabricWorkloadObject) prepareWorkloadObject).getChannel());
                    }
                    LOG.info("Closed listeners, finished " + clientObject.getClientId());
                }
                return;
            case Graphene:
                if (graphene.configuration.Configuration.DISCONNECT_WEBSOCKETS) {
                    for (final WebSocket webSocket :
                            ((PrepareGrapheneWorkloadObject) prepareWorkloadObject).getWebsocketList()) {
                        webSocket.disconnect();
                        LOG.info("Closed websocket, finished " + clientObject.getClientId());
                    }
                }
                return;
            default:
                LOG.error("Returning disconnectListeners due to default case");
        }

    }

}
