package fabric.workloads;

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
import fabric.configuration.Configuration;
import fabric.connection.FabricClient;
import fabric.helper.Utils;
import fabric.listener.Listener;
import fabric.payload_patterns.IFabricPayloads;
import fabric.payloads.IFabricReadPayload;
import fabric.payloads.IFabricWritePayload;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class PrepareFabricWorkload implements IPrepareWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(PrepareFabricWorkload.class);
    private static final AtomicBoolean FIRST_SET = new AtomicBoolean(false);
    private final List<PrepareFabricWorkloadObject> paramList = new ArrayList<>();

    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E prepareWorkload(final E... params) {

        checkFirstExecution();

        ClientObject clientObject = (ClientObject) params[0];

        FabricClient fabricClientUser =
                Utils.createSingleUserContext(clientObject.getClientId().substring(0,42) + System.currentTimeMillis());
        LOG.debug("Certificate: " + fabricClientUser.getInstance().getUserContext().getEnrollment().getCert());

        String channelName = GenericSelectionStrategy.selectFixed(Configuration.CHANNEL_LIST,
                Collections.singletonList(0), false).get(0);

        PrepareFabricWorkloadObject prepareFabricWorkloadObject = new PrepareFabricWorkloadObject();
        prepareFabricWorkloadObject.setFabricClient(fabricClientUser);
        prepareFabricWorkloadObject.prepareChannel(channelName);

        prepareFabricWorkloadObject.setPeerList(
                Collections.singletonList(GenericSelectionStrategy.selectFixed(Utils.getPeerList(),
                        Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])),
                        true).get(0)));
                //Collections.singletonList(GenericSelectionStrategy.selectRoundRobin(Utils.getPeerList(), 1, true, false, "rr-wl-peer", 1, false).get(0)));
        prepareFabricWorkloadObject.setOrdererList(
                Collections.singletonList(GenericSelectionStrategy.selectFixed(Utils.getOrdererList(),
                        Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])),
                        true).get(0)));
                //Collections.singletonList(GenericSelectionStrategy.selectRoundRobin(Utils.getOrdererList(), 1, true, false, "rr-wl-orderer", 1, false).get(0)));

        debug(fabricClientUser, prepareFabricWorkloadObject);

        prepareListener(clientObject, fabricClientUser, channelName, prepareFabricWorkloadObject);

        prepareWritePayloads(clientObject, prepareFabricWorkloadObject);
        prepareReadPayloads(clientObject, prepareFabricWorkloadObject);

        paramList.add(prepareFabricWorkloadObject);
        return (E) prepareFabricWorkloadObject;
    }

    @Suspendable
    private void debug(final FabricClient fabricClientUser,
                       final PrepareFabricWorkloadObject prepareFabricWorkloadObject) {
        if (Configuration.DEBUG_BLOCKCHAIN_INFO) {
            Utils.debugBlockchainInfo(prepareFabricWorkloadObject.getChannel(),
                    prepareFabricWorkloadObject.getChannel().getPeers(),
                    fabricClientUser.getInstance().getUserContext());
        }
    }

    @Suspendable
    private void prepareWritePayloads(final ClientObject clientObject,
                                      final PrepareFabricWorkloadObject prepareFabricWorkloadObject) {
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            List<List<IFabricWritePayload>> completeWritePayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                IFabricPayloads iFabricWritePayloadPattern = null;
                try {
                    iFabricWritePayloadPattern = Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<IFabricWritePayload> iFabricWritePayloads =
                        (List<IFabricWritePayload>) iFabricWritePayloadPattern.getPayloads(clientObject,
                                Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT);
                completeWritePayloadList.add(iFabricWritePayloads);
            }
            prepareFabricWorkloadObject.setFabricWritePayloads(completeWritePayloadList);
        }
    }

    @Suspendable
    private void prepareReadPayloads(final ClientObject clientObject,
                                     final PrepareFabricWorkloadObject prepareFabricWorkloadObject) {
        if (Configuration.PREPARE_READ_PAYLOADS) {
            List<List<IFabricReadPayload>> completeReadPayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                IFabricPayloads iFabricReadPayloadPattern = null;
                try {
                    iFabricReadPayloadPattern = Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<IFabricReadPayload> iFabricReadPayloads =
                        (List<IFabricReadPayload>) iFabricReadPayloadPattern.getPayloads(clientObject);
                completeReadPayloadList.add(iFabricReadPayloads);
            }
            prepareFabricWorkloadObject.setFabricReadPayloads(completeReadPayloadList);
        }
    }

    private static final AtomicInteger LISTENER_COUNTER = new AtomicInteger(0);

    @Suspendable
    private void prepareListener(final ClientObject clientObject, final FabricClient fabricClientUser,
                                 final String channelName,
                                 final PrepareFabricWorkloadObject prepareFabricWorkloadObject) {
        if (Configuration.ENABLE_LISTENER) {
            if (LISTENER_COUNTER.getAndIncrement() < Configuration.NUMBER_OF_LISTENERS) {
                LOG.info("Registering listener for: " + clientObject.getClientId());

                Listener listener =
                        new Listener(GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT,
                                GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS, Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT
                                , Configuration.LISTENER_THRESHOLD, Configuration.LISTENER_TOTAL_THRESHOLD, iStatistics);
                if (Configuration.LISTENER_AS_THREAD) {
                    Thread thread = new Thread(() -> {
                        prepareListenerLogic(clientObject, fabricClientUser, channelName, prepareFabricWorkloadObject,
                                listener);
                    });
                    thread.setName(clientObject.getClientId() + "-listener-thread");
                    thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    thread.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    thread.start();
                } else {
                    Fiber<Void> fiber = new Fiber<>(() -> {
                        prepareListenerLogic(clientObject, fabricClientUser, channelName, prepareFabricWorkloadObject,
                                listener);
                    });
                    fiber.setName(clientObject.getClientId() + "-listener-fiber");
                    fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    fiber.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    fiber.start();
                }

                try {
                    listener.getIsSubscribed().get(Configuration.TIMEOUT_LISTENER,
                            Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        }
    }

    @Suspendable
    private void prepareListenerLogic(final ClientObject clientObject, final FabricClient fabricClientUser,
                                      final String channelName,
                                      final PrepareFabricWorkloadObject prepareFabricWorkloadObject,
                                      final Listener listener) {
        String chaincodeListenerHandle = listener.registerChaincodeListener(fabricClientUser,
                Pattern.compile(".*"),
                Pattern.compile(".*"),
                channelName, ClientRegistry.getClientObjects());
        if(Configuration.ENABLE_BLOCK_STATISTICS) {
            String blockListenerHandle = listener.registerBlockListener(fabricClientUser, channelName, Collections.singletonList(clientObject));
            prepareFabricWorkloadObject.getListener().put(blockListenerHandle, false);
        }
        prepareFabricWorkloadObject.getListener().put(chaincodeListenerHandle, true);

        prepareFabricWorkloadObject.getIListenerDisconnectionLogicList().addAll(Arrays.asList(listener));
    }

    @Suspendable
    private static synchronized void checkFirstExecution() {
        if (!FIRST_SET.get()) {
            Utils.createUserContext();
            FIRST_SET.set(true);
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
