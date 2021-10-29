package fabric.workloads;

import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IListenerDisconnectionLogic;
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
import fabric.configuration.Configuration;
import fabric.listener.Listener;
import fabric.payload_patterns.IFabricPayloads;
import fabric.payloads.IFabricReadPayload;
import fabric.payloads.IFabricWritePayload;
import fabric.read.Read;
import fabric.statistics.ReadStatisticObject;
import fabric.statistics.WriteStatisticObject;
import fabric.write.Write;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.TransactionProposalRequest;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ExecuteFabricWorkload implements IExecuteWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(ExecuteFabricWorkload.class);
    private static final String READ_SUFFIX = "-read";
    private static final String WRITE_SUFFIX = "-write";
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private int writeRequests;
    private int readRequests;

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> E executeWorkload(final E... params) {

        ClientObject clientObject = (ClientObject) params[1];
        int workloadId = Integer.parseInt(String.valueOf(params[2])) + 1;

        List<PrepareFabricWorkloadObject> listOfWorkloadObjects =
                GenericSelectionStrategy.selectFixed(((ArrayList<PrepareFabricWorkloadObject>) params[0]),
                        Collections.singletonList(0), false);
        PrepareFabricWorkloadObject prepareWorkloadObject = listOfWorkloadObjects.get(0);

        if (Configuration.SEND_WRITE_REQUESTS) {
            List<IFabricWritePayload> iFabricWritePayloads = prepareWritePayloads(clientObject, listOfWorkloadObjects);

            Write fabricWrite = new Write();
            write(clientObject, workloadId, prepareWorkloadObject, iFabricWritePayloads, fabricWrite);
        }

        if (Configuration.SEND_READ_REQUESTS) {
            List<IFabricReadPayload> iFabricReadPayloads = prepareReadPayloads(clientObject, listOfWorkloadObjects);

            Read read = new Read();
            read(prepareWorkloadObject, clientObject, iFabricReadPayloads, read, workloadId);
        }

        // awaitEndOfExecution(prepareWorkloadObject);

        // unregisterListeners(clientObject, prepareWorkloadObject);

        return null;
    }

    private static final RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
            RateLimiter.create(
                    GenericSelectionStrategy.selectFixed(
                            Configuration.WRITE_PAYLOADS_PER_SECOND,
                            Collections.singletonList(0), false).get(0)
            ) : null;

    @Suspendable
    private void write(final ClientObject clientObject, final int workloadId,
                       final PrepareFabricWorkloadObject prepareWorkloadObject,
                       final List<IFabricWritePayload> iFabricWritePayloads, final Write fabricWrite) {
        prepareExpectedEventMap(clientObject, iFabricWritePayloads);

        /*RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.WRITE_PAYLOADS_PER_SECOND,
                                Collections.singletonList(0), false).get(0)
                ) : null;*/

        for (final IFabricWritePayload iFabricWritePayload : iFabricWritePayloads) {

            TransactionProposalRequest transactionRequest =
                    iFabricWritePayload.getTransactionRequest(prepareWorkloadObject.getFabricClient());

            Listener.getObtainedEventsMap().get(clientObject.getClientId()).get(iFabricWritePayload.getEventPrefix() + iFabricWritePayload.getSignature()).setLeft(System.nanoTime());

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(iFabricWritePayload.getEventPrefix() + iFabricWritePayload.getSignature(), m ->
                    MutablePair.of(System.currentTimeMillis(), -1L));

            //handleRequestDistribution(clientObject.getClientId());

            WriteStatisticObject writeStatisticObject = new WriteStatisticObject();

            writeStatisticObject.getAssocEventList().add(iFabricWritePayload.getEventPrefix() + iFabricWritePayload.getSignature());

            writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());

            write(prepareWorkloadObject, fabricWrite, transactionRequest,
                    writeStatisticObject, iFabricWritePayload, clientObject);
            writeStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            writeStatisticObject.setRequestNumber(++writeRequests);
            writeStatisticObject.setClientId(clientObject.getClientId());
            writeStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-write-" + writeRequests +
                    "-wlid" +
                    "-" + workloadId);
            writeStatisticObject.getSpecificPayloadTypeList().add(iFabricWritePayload.getSpecificPayloadType());
            iStatistics.add(writeStatisticObject);

        }
    }

    @Suspendable
    private void write(final PrepareFabricWorkloadObject prepareWorkloadObject, final Write fabricWrite,
                       final TransactionProposalRequest transactionRequest,
                       final WriteStatisticObject writeStatisticObject, final IFabricWritePayload iFabricWritePayload, final ClientObject clientObject) {

        boolean hasError;
        String hasMessage;
        int e = 0;
        boolean timeSet = false;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_WRITE;
        do {

            if (Configuration.ENABLE_RATE_LIMITER_FOR_WRITE_PAYLOADS) {
                rateLimiter.acquire();
                if (GeneralConfiguration.NOTE_RATE_LIMITER_WRITE == client.statistics.WriteStatisticObject.NoteRateLimiter.YES) {
                    if(!timeSet) {
                        writeStatisticObject.setCurrentTimeStart(System.currentTimeMillis());

                        Listener.getObtainedEventsMap().get(clientObject.getClientId()).get(iFabricWritePayload.getEventPrefix() + iFabricWritePayload.getSignature()).setLeft(System.nanoTime());

                        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                                ListenerReferenceValues.getTimeMap().computeIfAbsent(clientObject.getClientId(),
                                        c -> new ConcurrentHashMap<>());
                        stringMutablePairMap.computeIfAbsent(iFabricWritePayload.getEventPrefix() + iFabricWritePayload.getSignature(), m ->
                                MutablePair.of(System.currentTimeMillis(), -1L));
                        timeSet = true;
                    }
                }
            }

            ImmutablePair<Boolean, String> write = fabricWrite.write(transactionRequest,
                    prepareWorkloadObject.getChannel(), writeStatisticObject, prepareWorkloadObject.getPeerList(), prepareWorkloadObject.getOrdererList());

            hasError = write.getLeft();
            hasMessage = write.getRight();

            if (hasError) {
                LOG.error("Had error (write) resend " + e + " message " + hasMessage);
                e++;
            }

            if (hasError && hasMessage != null) {
                writeStatisticObject.getErrorMessages().add(hasMessage);
            }

            if(Configuration.DROP_ON_TIMEOUT && write.getRight().contains("TIMEOUT_EX")) {
                LOG.error("Dropping write request due to exception " + write.getRight());
                break;
            }

        } while (hasError && e < retries);
        LOG.info("Number of resends (write): " + e);

        if (hasError) {
            writeStatisticObject.setFailedRequest(true);
        }
    }

    @Suspendable
    private void prepareExpectedEventMap(final ClientObject clientObject,
                                         final List<IFabricWritePayload> iFabricWritePayloads) {

        for (final IFabricWritePayload iFabricWritePayload : iFabricWritePayloads) {

            String expectedEvent = iFabricWritePayload.getEventPrefix() + iFabricWritePayload.getSignature();

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    Listener.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                    MutablePair.of(System.nanoTime(), -1L));

        }
    }

    @Suspendable
    private static synchronized void unregisterListeners(final ClientObject clientObject,
                                                         final PrepareFabricWorkloadObject prepareWorkloadObject) {
        if (Configuration.UNREGISTER_LISTENERS) {
            for (final Map.Entry<String, Boolean> listenerEntry : prepareWorkloadObject.getListener().entrySet()) {
                Listener.unregisterAndUnsetAll(listenerEntry.getKey(), listenerEntry.getValue(),
                        prepareWorkloadObject.getChannel());
            }

            LOG.info("Closed listeners, finished " + clientObject.getClientId());
        }
    }

    @Suspendable
    private void awaitEndOfExecution(final PrepareFabricWorkloadObject prepareWorkloadObject) {
        for (final IListenerDisconnectionLogic iListenerDisconnectionLogic :
                prepareWorkloadObject.getIListenerDisconnectionLogicList()) {
            try {
                iListenerDisconnectionLogic.isDone().get(Configuration.TIMEOUT_LISTENER,
                        Configuration.TIMEOUT_LISTENER_TIME_UNIT);
                iStatistics.addAll(iListenerDisconnectionLogic.getStatistics());
            } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

    @Suspendable
    public void read(final PrepareFabricWorkloadObject prepareWorkloadObject, final ClientObject clientObject,
                     final List<IFabricReadPayload> iFabricReadPayloads, final Read read, final int workloadId) {

        RateLimiter rateLimiter = Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS ?
                RateLimiter.create(
                        GenericSelectionStrategy.selectFixed(
                                Configuration.READ_PAYLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)
                ) : null;

        for (final IFabricReadPayload iFabricReadPayload : iFabricReadPayloads) {

            if (Configuration.ENABLE_RATE_LIMITER_FOR_READ_PAYLOADS) {
                rateLimiter.acquire();
            }

            ReadStatisticObject readStatisticObject = new ReadStatisticObject();
            readStatisticObject.setCurrentTimeStart(System.currentTimeMillis());
            read(prepareWorkloadObject, clientObject, iFabricReadPayload, read, readStatisticObject);
            readStatisticObject.setCurrentTimeEnd(System.currentTimeMillis());
            readStatisticObject.setRequestNumber(++readRequests);
            readStatisticObject.setClientId(clientObject.getClientId());
            readStatisticObject.setRequestId("clid-" + clientObject.getClientId() + "-read-" + readRequests + "-wlid" +
                    "-" + workloadId);
            readStatisticObject.getSpecificPayloadTypeList().add(iFabricReadPayload.getSpecificPayloadType());
            iStatistics.add(readStatisticObject);

        }

    }

    @Suspendable
    private void read(final PrepareFabricWorkloadObject prepareWorkloadObject,
                      final ClientObject clientObject,
                      final IFabricReadPayload iFabricReadPayload,
                      final Read read,
                      final ReadStatisticObject readStatisticObject) {

        boolean hasError;
        String hasMessage;
        int e = 0;
        int retries = Configuration.RESEND_TIMES_UPON_ERROR_READ;
        do {

            ImmutablePair<Boolean, String> readRes =

                    read.read(prepareWorkloadObject.getFabricClient(), prepareWorkloadObject.getChannel(),
                            iFabricReadPayload.getPayload(),
                            readStatisticObject, null, null);

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
    private List<IFabricWritePayload> prepareWritePayloads(final ClientObject clientObject,
                                                           final List<PrepareFabricWorkloadObject> listOfWorkloadObjects) {
        List<IFabricWritePayload> iFabricWritePayloads;
        if (Configuration.PREPARE_WRITE_PAYLOADS) {
            String id = clientObject.getClientId() + WRITE_SUFFIX;
            List<List<IFabricWritePayload>> fabricWritePayloads = listOfWorkloadObjects.get(0).getFabricWritePayloads();
            iFabricWritePayloads = GenericSelectionStrategy.selectRoundRobin(fabricWritePayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            IFabricPayloads iFabricWritePayloadPattern = null;
            try {
                iFabricWritePayloadPattern = Configuration.WRITE_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iFabricWritePayloads =
                    (List<IFabricWritePayload>) iFabricWritePayloadPattern.getPayloads(clientObject,
                            Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT);
        }
        return iFabricWritePayloads;
    }

    @Suspendable
    private List<IFabricReadPayload> prepareReadPayloads(final ClientObject clientObject,
                                                         final List<PrepareFabricWorkloadObject> listOfWorkloadObjects) {
        List<IFabricReadPayload> iFabricReadPayloads;
        if (Configuration.PREPARE_READ_PAYLOADS) {
            String id = clientObject.getClientId() + READ_SUFFIX;
            List<List<IFabricReadPayload>> fabricReadPayloads = listOfWorkloadObjects.get(0).getFabricReadPayloads();
            iFabricReadPayloads = GenericSelectionStrategy.selectRoundRobin(fabricReadPayloads, 1, false, false,
                    id, 1, false).get(0);
        } else {

            IFabricPayloads iFabricReadPayloadPattern = null;
            try {
                iFabricReadPayloadPattern = Configuration.READ_PAYLOAD_PATTERN.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            iFabricReadPayloads =
                    (List<IFabricReadPayload>) iFabricReadPayloadPattern.getPayloads(clientObject);
        }
        return iFabricReadPayloads;
    }

    @Suspendable
    private void addToExpectedEventMap(final ClientObject clientObject,
                                       final String signature,
                                       final String expectedEventPrefix) {

        String expectedEvent = expectedEventPrefix + signature;

        Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                Listener.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                        c -> new ConcurrentHashMap<>());
        stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                MutablePair.of(System.nanoTime(), -1L));

    }

    @SafeVarargs
    @Override
    @Suspendable
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
