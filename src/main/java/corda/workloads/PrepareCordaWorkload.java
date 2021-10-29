package corda.workloads;

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
import corda.configuration.Configuration;
import corda.connection.Rpc;
import corda.helper.Helper;
import corda.helper.PartyMap;
import corda.listener.Listen;
import corda.payload_patterns.ICordaPayloads;
import corda.payloads.ICordaReadPayload;
import corda.payloads.ICordaWritePayload;
import corda.supplements.CordaDebugHelper;
import net.corda.client.rpc.CordaRPCClient;
import net.corda.core.messaging.CordaRPCOps;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class PrepareCordaWorkload implements IPrepareWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(PrepareCordaWorkload.class);
    private final List<Object> paramList = new ArrayList<>();
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> E prepareWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[0];

        CordaRPCClient cordaRPCClient = new Rpc().prepareRpcConnection(
                /*GenericSelectionStrategy.selectFixed(
                        Configuration.NODE_LIST, Collections.singletonList(0), false));*/
                /*Collections.singletonList(GenericSelectionStrategy.selectRoundRobin(
                        Configuration.NODE_LIST, 1, true, false, "rr-wl", 1, false).get(0)));*/
                GenericSelectionStrategy.selectFixed(Configuration.NODE_LIST, Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])), true));

        CordaRPCOps proxy = new Rpc().startRpcClient(Configuration.RPC_USER, Configuration.RPC_PASSWORD,
                cordaRPCClient);

        debug(proxy);

        setTxId(clientObject, proxy);

        PrepareCordaWorkloadObject prepareCordaWorkloadObject = new PrepareCordaWorkloadObject();

        prepareCordaWorkloadObject.setProxy(proxy);

        paramList.add(prepareCordaWorkloadObject);

        PartyMap.getNotariesAsStringsUnsorted().put(clientObject.getClientId(), Helper.getNotariesAsStringList(proxy));
        PartyMap.getPartiesWithoutNotariesUnsorted().put(clientObject.getClientId(), Helper.getPartiesWithoutNotaries(proxy));
        PartyMap.getOnlyNotariesUnsorted().put(clientObject.getClientId(), Helper.getOnlyNotaries(proxy));
        PartyMap.getPartiesUnsorted().put(clientObject.getClientId(), Helper.getParties(proxy, false));

        prepareListener(clientObject, prepareCordaWorkloadObject);

        prepareWritePayloads(proxy, prepareCordaWorkloadObject, clientObject);
        prepareReadPayloads(prepareCordaWorkloadObject, clientObject);

        return (E) prepareCordaWorkloadObject;
    }

    private static final AtomicInteger LISTENER_COUNTER = new AtomicInteger(0);

    @Suspendable
    private void setTxId(final ClientObject clientObject, final CordaRPCOps proxy) {
        if (Configuration.SET_TRANSACTION_ID) {
            Listen listen = new Listen();
            listen.stateMachineRecordedTransactionMappingFeed(proxy, clientObject.getClientId());

            /*QueryCriteria.VaultQueryCriteria vaultQueryCriteria = new QueryCriteria.VaultQueryCriteria(
                    Vault.StateStatus.UNCONSUMED, null, null, null, null,
                    null, Vault.RelevancyStatus.ALL, Collections.EMPTY_SET, Collections.EMPTY_SET,
                    null, Collections.EMPTY_LIST, null);
            PageSpecification pageSpecification = new PageSpecification(Configuration.DEFAULT_PAGE_NUMBER,
                    Configuration.DEFAULT_PAGE_SIZE);
            Sort sort = new Sort(Collections.emptyList());
            listen.vaultTrackBy(proxy, State.class, vaultQueryCriteria, pageSpecification, sort);*/
        }
    }

    @Suspendable
    private void prepareListener(final ClientObject clientObject,
                                 final PrepareCordaWorkloadObject prepareCordaWorkloadObject) {
        if (Configuration.ENABLE_LISTENER) {
            if (LISTENER_COUNTER.getAndIncrement() < Configuration.NUMBER_OF_LISTENERS) {
                LOG.info("Registering listener for: " + clientObject.getClientId());

                Listen listen =
                        new Listen(GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT,
                                GeneralConfiguration.CLIENT_COUNT * GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS, Collections.singletonList(0), false).get(0) * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT
                                , Configuration.LISTENER_THRESHOLD, Configuration.LISTENER_TOTAL_THRESHOLD, iStatistics, /*Collections.singletonList(clientObject.getClientId())*/ ClientRegistry.getClientObjects());
                if (Configuration.LISTENER_AS_THREAD) {
                    Thread thread = new Thread(() -> {
                        prepareListenerLogic(clientObject, prepareCordaWorkloadObject,
                                listen);
                    });
                    thread.setName(clientObject.getClientId() + "-listener-thread");
                    thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    thread.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    thread.start();
                } else {
                    Fiber<Void> fiber = new Fiber<>(() -> {
                        prepareListenerLogic(clientObject, prepareCordaWorkloadObject,
                                listen);
                    });
                    fiber.setName(clientObject.getClientId() + "-listener-fiber");
                    fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    fiber.setPriority(GeneralConfiguration.LISTENER_PRIORITY);
                    fiber.start();
                }

                try {
                    listen.getIsSubscribed().get(Configuration.TIMEOUT_LISTENER,
                            Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        }
    }

    @Suspendable
    private void prepareListenerLogic(final ClientObject clientObject,
                                      final PrepareCordaWorkloadObject prepareCordaWorkloadObject,
                                      final Listen listener) {

        listener.prepareListenForEvents();

        prepareCordaWorkloadObject.getListener().put(clientObject.getClientId(), listener);

        prepareCordaWorkloadObject.getIListenerDisconnectionLogicList().addAll(Arrays.asList(listener));
    }

    @Suspendable
    private void debug(final CordaRPCOps proxy) {
        if (Configuration.PRINT_DEBUG) {
            CordaDebugHelper.printDebug(proxy);
        }
    }

    @Suspendable
    private void prepareWritePayloads(final CordaRPCOps proxy,
                                      final PrepareCordaWorkloadObject prepareCordaWorkloadObject,
                                      final ClientObject clientObject) {
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            List<List<ICordaWritePayload>> completeWritePayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                ICordaPayloads iCordaWritePayloadPattern = null;
                try {
                    iCordaWritePayloadPattern = Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<ICordaWritePayload> iCordaWritePayloads =
                        (List<ICordaWritePayload>) iCordaWritePayloadPattern.getPayloads(proxy, clientObject,
                                Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT);
                completeWritePayloadList.add(iCordaWritePayloads);
            }

            prepareCordaWorkloadObject.setCordaWritePayloads(completeWritePayloadList);
        }
    }

    @Suspendable
    private void prepareReadPayloads(final PrepareCordaWorkloadObject prepareCordaWorkloadObject,
                                     final ClientObject clientObject) {
        if (Configuration.PREPARE_READ_PAYLOADS) {
            List<List<ICordaReadPayload>> completeReadPayloadList = new ArrayList<>();
            for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0); i++) {

                ICordaPayloads iCordaReadPayloadPattern = null;
                try {
                    iCordaReadPayloadPattern = Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                    ExceptionHandler.logException(ex);
                }

                List<ICordaReadPayload> iCordaReadPayloads =
                        (List<ICordaReadPayload>) iCordaReadPayloadPattern.getPayloads(clientObject);
                completeReadPayloadList.add(iCordaReadPayloads);
            }
            prepareCordaWorkloadObject.setCordaReadPayloads(completeReadPayloadList);
        }
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleRequestDistribution(final E... params) {
    }

    @Suspendable
    public <E> List<E> getParams() {
        return (List<E>) paramList;
    }

    @Suspendable
    @SafeVarargs
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

}
