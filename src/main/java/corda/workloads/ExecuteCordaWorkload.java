package corda.workloads;

import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.statistics.ListenerReferenceValues;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import client.utils.NumberGenerator;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.google.common.util.concurrent.RateLimiter;
import corda.configuration.Configuration;
import corda.connection.Rpc;
import corda.helper.Helper;
import corda.helper.PartyMap;
import corda.listener.Listen;
import corda.payload_patterns.ICordaPayloads;
import corda.payloads.ICordaReadPayload;
import corda.payloads.ICordaWritePayload;
import corda.read.Read;
import corda.statistics.ReadStatisticObject;
import corda.statistics.WriteStatisticObject;
import corda.write.Write;
import net.corda.client.rpc.CordaRPCClient;
import net.corda.core.flows.FlowLogic;
import net.corda.core.identity.Party;
import net.corda.core.messaging.CordaRPCOps;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ExecuteCordaWorkload implements IExecuteWorkload, IRequestDistribution {

    private static final Map<CordaRPCOps, List<String>> PROXY_MAP = new ConcurrentHashMap<>();
    private static final Logger LOG = Logger.getLogger(ExecuteCordaWorkload.class);
    private static final String WRITE_SUFFIX = "-write";
    private static final String READ_SUFFIX = "-read";
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private int writeRequests;
    private int readRequests;

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E executeWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[1];
        int workloadId = Integer.parseInt(String.valueOf(params[2])) + 1;

        List<PrepareCordaWorkloadObject> listOfWorkloadObjects =
                GenericSelectionStrategy.selectFixed(((ArrayList<PrepareCordaWorkloadObject>) params[0]),
                        Collections.singletonList(0), false);

        CordaRPCOps proxy = getCordaRPCOps(listOfWorkloadObjects, clientObject.getClientId());

        PROXY_MAP.computeIfAbsent(proxy, l -> {
                    List<String> tmp = new ArrayList<>();
                    proxy.nodeInfo().getAddresses().forEach(nodeInfo ->
                            tmp.add(nodeInfo.toString() + "|Proxy")
                    );
                    return tmp;
                }
        );

        if (Configuration.SEND_WRITE_REQUESTS) {

            List<ICordaWritePayload> iCordaWritePayloads = prepareWritePayloads(clientObject, listOfWorkloadObjects,
                    proxy, workloadId);

            Write write = new Write();
            write(clientObject, workloadId, proxy, iCordaWritePayloads, write);
        }

        if (Configuration.SEND_READ_REQUESTS) {
            List<ICordaReadPayload> iCordaReadPayloads = prepareReadPayloads(clientObject, listOfWorkloadObjects);

            Read read = new Read();
            read(clientObject, proxy, iCordaReadPayloads, read, workloadId);
        }

        return null;
    }

    private static final RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
            RateLimiter.create(
                    GenericSelectionStrategy.selectFixed(
                            Configuration.WRITE_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
            ) : null;

    @Suspendable
    private <E> void write(final ClientObject clientObject, final int workloadId, final CordaRPCOps proxy,
                           final List<ICordaWritePayload> iCordaWritePayloads, final Write write) {

        prepareExpectedEventMap(clientObject, iCordaWritePayloads);

        /*RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.WRITE_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
                ) : null;*/

        for (final ICordaWritePayload iCordaWritePayload : iCordaWritePayloads) {

            Pair<Class<FlowLogic<?>>, List<E>> pair = iCordaWritePayload.getPayload();

            Listen.getObtainedEventsMap().get(clientObject.getClientId()).get(iCordaWritePayload.getEventPrefix() + iCordaWritePayload.getSignature()).setLeft(System.nanoTime());

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(iCordaWritePayload.getEventPrefix() + iCordaWritePayload.getSignature(), m ->
                    MutablePair.of(System.currentTimeMillis(), -1L));

            //handleRequestDistribution(clientObject.getClientId());

            WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

            writeStatisticObject.getAssocEventList().add(iCordaWritePayload.getEventPrefix() + iCordaWritePayload.getSignature());

            for (final Party party : iCordaWritePayload.getParties()) {
                writeStatisticObject.getParticipatingServers().add(party.toString() + "|SigningParty");
            }
            writeStatisticObject.getParticipatingServers().add(iCordaWritePayload.getNotary().toString() + "|Notary");

            writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
            write(clientObject, proxy, pair, write, writeStatisticObject, iStatistics, iCordaWritePayload);
            writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            writeStatisticObject.setRequestNumber(++writeRequests);
            writeStatisticObject.setClientId(clientObject.getClientId());
            writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests +
                    "-wlid" +
                    "-" + workloadId);
            writeStatisticObject.getSpecificPayloadTypeList().add(iCordaWritePayload.getSpecificPayloadType());
            iStatistics.add(writeStatisticObject);
        }
    }

    @Suspendable
    private void prepareExpectedEventMap(final ClientObject clientObject,
                                         final List<ICordaWritePayload> iCordaWritePayloads) {

        for (final ICordaWritePayload iCordaWritePayload : iCordaWritePayloads) {

            String expectedEvent = iCordaWritePayload.getEventPrefix() + iCordaWritePayload.getSignature();

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    Listen.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                    MutablePair.of(System.nanoTime(), -1L));

        }
    }

    @Suspendable
    private <E> void write(final ClientObject clientObject, final CordaRPCOps proxy, final Pair<Class<FlowLogic<?>>,
            List<E>> pair,
                           final Write write,
                           final WriteStatisticObject writeStatisticObject,
                           final Queue<IStatistics> iStatistics, final ICordaWritePayload iCordaWritePayload) {
        boolean hasError;
        String hasMessage;
        int e = 0;
        boolean timeSet = false;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_WRITE;

        do {

            PROXY_MAP.get(proxy).forEach(n ->
                    writeStatisticObject.getParticipatingServers().add(n));
            /*proxy.nodeInfo().getAddresses().forEach(nodeInfo ->
                    writeStatisticObject.getParticipatingServers().add(nodeInfo.toString() + "|Proxy")
            );*/

            if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                rateLimiter.acquire();
                if (GeneralConfiguration.NOTE_RATE_LIMITER_WRITE == client.statistics.WriteStatisticObject.NoteRateLimiter.YES) {
                    if(!timeSet) {
                        writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());

                        Listen.getObtainedEventsMap().get(clientObject.getClientId()).get(iCordaWritePayload.getEventPrefix() + iCordaWritePayload.getSignature()).setLeft(System.nanoTime());

                        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                                ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                                        c -> new ConcurrentHashMap<>());
                        stringMutablePairMap.computeIfAbsent(iCordaWritePayload.getEventPrefix() + iCordaWritePayload.getSignature(), m ->
                                MutablePair.of(System.currentTimeMillis(), -1L));
                        timeSet = true;
                    }
                }
            }

            ImmutablePair<Boolean, String> writeRes = write.write(proxy, pair, writeStatisticObject,
                    iStatistics,
                    clientObject.getClientId(), Configuration.LISTENER_THRESHOLD,
                    Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT *
                            GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                    Collections.singletonList(0), false).get(0) * GeneralConfiguration.CLIENT_COUNT);

            hasError = writeRes.getLeft();
            hasMessage = writeRes.getRight();

            if (hasError) {
                LOG.error("Had error (write) resend " + e + " message " + hasMessage);
                e++;
            }

            if (hasError && hasMessage != null) {
                writeStatisticObject.getErrorMessages().add(hasMessage);
            }

            if(Configuration.DROP_ON_TIMEOUT && writeRes.getRight().contains("TIMEOUT_EX")) {
                LOG.error("Dropping write request due to exception " + writeRes.getRight());
                break;
            }

        } while (hasError && e < retries);
        LOG.info("Number of resends (write): " + e);

        if (hasError) {
            writeStatisticObject.setFailedRequest(true);
        }
    }

    @Suspendable
    private void read(final ClientObject clientObject, final CordaRPCOps proxy,
                      final List<ICordaReadPayload> iCordaReadPayloads,
                      final Read read,
                      final int workloadId) {

        RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.READ_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
                ) : null;

        for (final ICordaReadPayload iCordaReadPayload : iCordaReadPayloads) {
            if (Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS) {
                rateLimiter.acquire();
            }

            ReadStatisticObject readStatisticObject = new ReadStatisticObject();
            readStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
            read(proxy, iCordaReadPayload, read, readStatisticObject);
            readStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            readStatisticObject.setRequestNumber(++readRequests);
            readStatisticObject.setClientId(clientObject.getClientId());
            readStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-read-" + readRequests + "-wlid" +
                    "-" + workloadId);
            readStatisticObject.getSpecificPayloadTypeList().add(iCordaReadPayload.getSpecificPayloadType());
            iStatistics.add(readStatisticObject);
        }

    }

    @Suspendable
    private void read(final CordaRPCOps proxy,
                      final ICordaReadPayload iCordaReadPayload,
                      final Read read,
                      final ReadStatisticObject readStatisticObject) {
        boolean hasError;
        String hasMessage;
        int e = 0;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_READ;

        do {

            PROXY_MAP.get(proxy).forEach(n ->
                    readStatisticObject.getParticipatingServers().add(n));
            /*proxy.nodeInfo().getAddresses().forEach(nodeInfo ->
                    readStatisticObject.getParticipatingServers().add(nodeInfo.toString() + "|Proxy")
            );*/

            ImmutablePair<Boolean, String> readRes = read.read(proxy,
                    iCordaReadPayload,
                    readStatisticObject);

            hasError = readRes.getLeft();
            hasMessage = readRes.getRight();

            if (hasError) {
                LOG.error("Had error (read) resend " + e + " message " + hasMessage);
                e++;
            }

            if (hasError && hasMessage != null) {
                readStatisticObject.getErrorMessages().add(hasMessage);
            }

            if(Configuration.DROP_ON_TIMEOUT && readRes.getRight().contains("TIMEOUT_EX")) {
                LOG.error("Dropping read request due to exception " + readRes.getRight());
                break;
            }

        } while (hasError && e < retries);
        LOG.info("Number of resends (read): " + e);

        if (hasError) {
            readStatisticObject.setFailedRequest(true);
        }
    }

    @Suspendable
    private List<ICordaWritePayload> prepareWritePayloads(final ClientObject clientObject,
                                                          final List<PrepareCordaWorkloadObject> listOfWorkloadObjects,
                                                          final CordaRPCOps proxy, final int workloadId) {
        List<ICordaWritePayload> iCordaWritePayloads;
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            String id = clientObject.getClientId() + WRITE_SUFFIX;
            List<List<ICordaWritePayload>> cordaWritePayloads = listOfWorkloadObjects.get(0).getCordaWritePayloads();
            iCordaWritePayloads = GenericSelectionStrategy.selectRoundRobin(cordaWritePayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            ICordaPayloads iCordaWritePayloadPattern = null;
            try {
                iCordaWritePayloadPattern = Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iCordaWritePayloads =
                    (List<ICordaWritePayload>) iCordaWritePayloadPattern.getPayloads(proxy, clientObject,
                            Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT);
        }
        return iCordaWritePayloads;
    }

    @Suspendable
    private List<ICordaReadPayload> prepareReadPayloads(final ClientObject clientObject,
                                                        final List<PrepareCordaWorkloadObject> listOfWorkloadObjects) {
        List<ICordaReadPayload> iCordaReadPayloads;
        if (Configuration.PREPARE_READ_PAYLOADS) {
            String id = clientObject.getClientId() + READ_SUFFIX;
            List<List<ICordaReadPayload>> cordaReadPayloads = listOfWorkloadObjects.get(0).getCordaReadPayloads();
            iCordaReadPayloads = GenericSelectionStrategy.selectRoundRobin(cordaReadPayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            ICordaPayloads iCordaReadPayloadPattern = null;
            try {
                iCordaReadPayloadPattern = Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iCordaReadPayloads =
                    (List<ICordaReadPayload>) iCordaReadPayloadPattern.getPayloads(clientObject);
        }
        return iCordaReadPayloads;
    }

    @Suspendable
    private CordaRPCOps getCordaRPCOps(final List<PrepareCordaWorkloadObject> listOfWorkloadObjects,
                                       final String clientId) {
        CordaRPCOps proxy;
        if (Configuration.USE_PREPARED_PROXY) {
            proxy = listOfWorkloadObjects.get(0).getProxy();
        } else {
            CordaRPCClient cordaRPCClient = new Rpc().prepareRpcConnection(
                    /*GenericSelectionStrategy.selectFixed(
                            Configuration.NODE_LIST, Collections.singletonList(0), false));*/
                    /*GenericSelectionStrategy.selectRoundRobin(
                            Configuration.NODE_LIST, 1, true, false, "rr-wl", 1, false));*/
                    GenericSelectionStrategy.selectFixed(Configuration.NODE_LIST, Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[2])), true));


            proxy = new Rpc().startRpcClient(Configuration.RPC_USER, Configuration.RPC_PASSWORD,
                    cordaRPCClient);

            PartyMap.getNotariesAsStringsUnsorted().put(clientId, Helper.getNotariesAsStringList(proxy));
            PartyMap.getPartiesWithoutNotariesUnsorted().put(clientId, Helper.getPartiesWithoutNotaries(proxy));
            PartyMap.getOnlyNotariesUnsorted().put(clientId, Helper.getOnlyNotaries(proxy));
            PartyMap.getPartiesUnsorted().put(clientId, Helper.getParties(proxy, false));
        }
        return proxy;
    }

    @Suspendable
    private void addToExpectedEventMap(final ClientObject clientObject,
                                       final String signature,
                                       final String expectedEventPrefix) {

        String expectedEvent = expectedEventPrefix + signature;

        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                Listen.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                        c -> new ConcurrentHashMap<>());
        stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                MutablePair.of(System.nanoTime(), -1L));

    }

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> E endWorkload(final E... params) {
        LOG.info(((ClientObject) params[1]).getClientId() + " client ended");
        return null;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public synchronized final <E> Queue<IStatistics> getStatistics(final E... params) {
        return iStatistics;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void handleRequestDistribution(final E... params) {
        try {
            int randomSleep = NumberGenerator.selectRandomAsInt(50, 250);
            LOG.debug("Sleep time: " + randomSleep + " for " + params[0]);
            Strand.sleep(randomSleep);
        } catch (SuspendExecution | InterruptedException ex) {
            ExceptionHandler.logException(ex);
        }
    }
}
