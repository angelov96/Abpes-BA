package diem.workloads;

import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.statistics.IStatistics;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import client.utils.NumberGenerator;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import diem.listener.Listener;
import diem.payloads.IDiemReadPayload;
import diem.payloads.IDiemWritePayload;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.log4j.Logger;
import quorum.configuration.Configuration;
import quorum.read.Read;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecuteDiemWorkload implements IExecuteWorkload, IRequestDistribution {
    private static final Logger LOG = Logger.getLogger(ExecuteDiemWorkload.class);
    private static final Map<String, AtomicInteger> SUCCESSFUL_WRITE_REQUESTS =
            new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> UNSUCCESSFUL_WRITE_REQUESTS =
            new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> SUCCESSFUL_READ_REQUESTS =
            new ConcurrentHashMap<>();
    private static final Map<String, AtomicInteger> UNSUCCESSFUL_READ_REQUESTS =
            new ConcurrentHashMap<>();
    private static final String READ_SUFFIX = "-read";
    private static final String WRITE_SUFFIX = "-write";

    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();

    private int writeRequests;
    private int readRequests;

    @Override
    public <E> E executeWorkload(E... params) {
        ClientObject clientObject = (ClientObject) params[1];
        int workloadId = Integer.parseInt(String.valueOf(params[2])) + 1;

        List<PrepareDiemWorkloadObject> listOfWorkloadObjects =
                GenericSelectionStrategy.selectFixed(((ArrayList<PrepareDiemWorkloadObject>) params[0]),
                        Collections.singletonList(0), false);
        PrepareDiemWorkloadObject prepareWorkloadObject = listOfWorkloadObjects.get(0);

        if (Configuration.SEND_WRITE_REQUESTS) {
            List<IDiemWritePayload> iDiemWritePayloads = prepareWritePayloads(clientObject, listOfWorkloadObjects);

            prepareExpectedEventMap(clientObject, iDiemWritePayloads);

            write(clientObject, prepareWorkloadObject, iDiemWritePayloads, workloadId);
        }

        if (Configuration.SEND_READ_REQUESTS) {
            List<IDiemReadPayload> iDiemReadPayloads = prepareReadPayloads(clientObject, listOfWorkloadObjects);

            Read read = new Read();
            read(clientObject, prepareWorkloadObject, iDiemReadPayloads, read, workloadId);

        }
        return null;
    }

    private List<IDiemReadPayload> prepareReadPayloads(ClientObject clientObject,
                                                       List<PrepareDiemWorkloadObject> listOfWorkloadObjects) {
        return null;
    }

    private List<IDiemWritePayload> prepareWritePayloads(ClientObject clientObject,
                                                         List<PrepareDiemWorkloadObject> listOfWorkloadObjects) {
        return null;
    }

    private void prepareExpectedEventMap(ClientObject clientObject,
                                         List<IDiemWritePayload> iDiemWritePayloads) {
        for (final IDiemWritePayload iDiemWritePayload : iDiemWritePayloads) {
            String expectedEvent = iDiemWritePayload.getEventPrefix() + iDiemWritePayload.getSignature();

            Map<String, MutablePair<Long, Long>> stringMutablePairMap =
                    Listener.getObtainedEventsMap().computeIfAbsent(clientObject.getClientId(),
                            c -> new ConcurrentHashMap<>());
            stringMutablePairMap.computeIfAbsent(expectedEvent, m ->
                                                  MutablePair.of(System.nanoTime(), -1L));
        }

    }

    @Override
    public <E> E endWorkload(E... params) {
        LOG.info(((ClientObject) params[1]).getClientId() + " client ended");
        return null;
    }

    @Override
    public <E> Queue<IStatistics> getStatistics(E... params) {
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

    @Suspendable
    private void write(ClientObject clientObject, PrepareDiemWorkloadObject prepareWorkloadObject,
                       List<IDiemWritePayload> iDiemWritePayloads, int workloadId) {
    }

    @Suspendable
    private void read(final ClientObject clientObject, final PrepareDiemWorkloadObject prepareWorkloadObject,
                      final List<IDiemReadPayload> iQuorumReadPayloads, final Read read, final int workloadId) {

    }
}
