package sawtooth.workloads;

import client.client.ClientObject;
import client.client.ClientRegistry;
import client.commoninterfaces.IPrepareWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.zeromq.ZMQ;
import sawtooth.components.SawtoothSigner;
import sawtooth.configuration.Configuration;
import sawtooth.connection.ConnectionEnum;
import sawtooth.connection.ZmqConnection;
import sawtooth.listener.WebsocketListener;
import sawtooth.listener.ZmqListener;
import sawtooth.payload_patterns.ISawtoothPayloads;
import sawtooth.payloads.ISawtoothReadPayload;
import sawtooth.payloads.ISawtoothWritePayload;
import sawtooth.read.ReadWebsocket;
import sawtooth.sdk.signing.Signer;
import sawtooth.write.WriteWebsocket;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class PrepareSawtoothWorkload implements IPrepareWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(PrepareSawtoothWorkload.class);
    private static final boolean SUBSCRIBE_VIA_WEBSOCKET = true;
    private final List<PrepareSawtoothWorkloadObject> paramList = new ArrayList<>();
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E prepareWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[0];

        PrepareSawtoothWorkloadObject prepareSawtoothWorkloadObject = new PrepareSawtoothWorkloadObject();

        Signer signer = new SawtoothSigner(clientObject.getClientId()).getSigner();
        prepareSawtoothWorkloadObject.setSigner(signer);

        if (Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.ZMQ) {
            prepareSawtoothWorkloadObject.setServerAddressesWrite(
                    GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ), Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])), true)
                    /*GenericSelectionStrategy.selectRoundRobin(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ),
                    1, true, false, "rr-wl", 1, false)*/
            );
        }
        else if (Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.WebSocket) {
            prepareSawtoothWorkloadObject.setServerAddressesWrite(
                    GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET), Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])), true)
                    /*GenericSelectionStrategy.selectRoundRobin(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_WEBSOCKET),
                    1, true, false, "rr-wl", 1, false)*/
            );
        }

        prepareListener(clientObject, prepareSawtoothWorkloadObject);

        if (Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.ZMQ) {
            LOG.debug("Using ZMQ");
        } else if (Configuration.CONNECTION_TYPE_WRITE == ConnectionEnum.WebSocket) {
            LOG.debug("Using Websocket");
            WriteWebsocket writeWebsocket = new WriteWebsocket();
            prepareSawtoothWorkloadObject.setWriteWebsocket(writeWebsocket);
            ReadWebsocket readWebsocket = new ReadWebsocket();
            prepareSawtoothWorkloadObject.setReadWebsocket(readWebsocket);
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }

        prepareWritePayloads(clientObject, prepareSawtoothWorkloadObject);
        prepareReadPayloads(clientObject, prepareSawtoothWorkloadObject);

        paramList.add(prepareSawtoothWorkloadObject);
        return (E) prepareSawtoothWorkloadObject;
    }

    @Suspendable
    private void prepareWritePayloads(final ClientObject clientObject,
                                      final PrepareSawtoothWorkloadObject prepareSawtoothWorkloadObject) {
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            List<List<ISawtoothWritePayload>> completeWritePayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                ISawtoothPayloads iSawtoothWritePayloadPattern = null;
                try {
                    iSawtoothWritePayloadPattern =
                            Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<ISawtoothWritePayload> iSawtoothWritePayloads =
                        (List<ISawtoothWritePayload>) iSawtoothWritePayloadPattern.getPayloads(clientObject,
                                Configuration.NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT);
                completeWritePayloadList.add(iSawtoothWritePayloads);
            }
            prepareSawtoothWorkloadObject.setSawtoothWritePayloads(completeWritePayloadList);
        }
    }

    @Suspendable
    private void prepareReadPayloads(final ClientObject clientObject,
                                     final PrepareSawtoothWorkloadObject prepareSawtoothWorkloadObject) {
        if (Configuration.PREPARE_READ_PAYLOADS) {
            List<List<ISawtoothReadPayload>> completeReadPayloadList = new ArrayList<>();
            for (int i = 0; i < Configuration.READ_REQUESTS; i++) {

                ISawtoothPayloads iSawtoothReadPayloadPattern = null;
                try {
                    iSawtoothReadPayloadPattern =
                            Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<ISawtoothReadPayload> iSawtoothReadPayloads =
                        (List<ISawtoothReadPayload>) iSawtoothReadPayloadPattern.getPayloads(clientObject);
                completeReadPayloadList.add(iSawtoothReadPayloads);
            }
            prepareSawtoothWorkloadObject.setSawtoothReadPayloads(completeReadPayloadList);
        }
    }

    private static final AtomicInteger LISTENER_COUNTER = new AtomicInteger(0);

    @Suspendable
    private void prepareListener(final ClientObject clientObject,
                                 final PrepareSawtoothWorkloadObject prepareSawtoothWorkloadObject) {
        if (Configuration.ENABLE_LISTENER) {
            if (LISTENER_COUNTER.getAndIncrement() < Configuration.NUMBER_OF_LISTENERS) {
                LOG.info("Registering listener for: " + clientObject.getClientId());

                Queue<ClientObject> clientObjects = ClientRegistry.getClientObjects();

                ZmqListener zmqListener = null;
                if (Configuration.ZMQ_LISTENER) {
                    zmqListener =
                            new ZmqListener(GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS, Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT,
                                    GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS, Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTION_PAYLOADS_PER_CLIENT,
                                    Configuration.LISTENER_THRESHOLD, Configuration.LISTENER_TOTAL_THRESHOLD,
                                    iStatistics);
                }
                if (Configuration.LISTENER_AS_THREAD) {
                    ZmqListener finalZmqListener = zmqListener;
                    Thread thread = new Thread(() -> prepareListenerLogic(clientObject, clientObjects, prepareSawtoothWorkloadObject,
                            finalZmqListener));
                    thread.setName(clientObject.getClientId() + "-listener-thread");
                    thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    thread.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    thread.start();
                } else {
                    ZmqListener finalZmqListener = zmqListener;
                    Fiber<Void> fiber = new Fiber<>(() -> prepareListenerLogic(clientObject, clientObjects,
                            prepareSawtoothWorkloadObject, finalZmqListener));
                    fiber.setName(clientObject.getClientId() + "-listener-fiber");
                    fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    fiber.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    fiber.start();
                }

                if (Configuration.ZMQ_LISTENER) {
                    try {
                        zmqListener.getIsSubscribed().get(Configuration.TIMEOUT_LISTENER,
                                Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                    } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                        ExceptionHandler.logException(ex);
                    }
                }

            }
        }
    }

    @Suspendable
    private void prepareListenerLogic(final ClientObject clientObject, final Queue<ClientObject> clientObjects,
                                      final PrepareSawtoothWorkloadObject prepareSawtoothWorkloadObject, final
                                      ZmqListener zmqListener) {

        if (Configuration.WEBSOCKET_LISTENER) {
            WebsocketListener websocketListener = new WebsocketListener(clientObjects);

            String websocketSubscriptionServer =
                    GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SUBSCRIBE_TO_WS),
                            Collections.singletonList(0), false).get(0);

            websocketListener.createWebsocketListener(websocketSubscriptionServer, SUBSCRIBE_VIA_WEBSOCKET);

            prepareSawtoothWorkloadObject.getWebSocketSubscriptionServers().add(websocketSubscriptionServer);

            prepareSawtoothWorkloadObject.getIListenerDisconnectionLogicList().add(websocketListener);
        }
        if (Configuration.ZMQ_LISTENER) {

            String zmqSubscriptionServer =
                    GenericSelectionStrategy.selectFixed(Arrays.asList(Configuration.VALIDATORS_TO_SEND_TRANSACTIONS_TO_ZMQ),
                            Collections.singletonList(0), false).get(0);

            ZmqConnection zmqConnection = new ZmqConnection();
            ZMQ.Socket socket = zmqConnection.createZmqListener();
            zmqConnection.connectToZmq(socket,
                    zmqSubscriptionServer);

            String correspondingId = clientObject.getClientId() + "-zmq-listener";

            prepareSawtoothWorkloadObject.getZmqSocketSubscriptionServerMap().put(zmqSubscriptionServer, socket);

            prepareSawtoothWorkloadObject.getIListenerDisconnectionLogicList().add(zmqListener);

            ZmqListener.subscribeListener(socket, correspondingId);

            zmqListener.receiveZmq(socket, clientObjects /*ClientRegistry
            .getClientObjects()*/,
                    Configuration.LISTENER_SLEEP_TIME);

        }

    }

    @Suspendable
    public <E> List<E> getParams() {
        return (List<E>) paramList;
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E endPrepareWorkload(final E... params) {
        LOG.info(((ClientObject) params[0]).getClientId() + " client preparation ended");
        return null;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final synchronized <E> Queue<IStatistics> getStatistics(final E... params) {
        return iStatistics;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleRequestDistribution(final E... params) {
    }
}
