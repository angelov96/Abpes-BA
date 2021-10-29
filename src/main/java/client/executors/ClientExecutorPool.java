package client.executors;

import client.blockchain.BlockchainFramework;
import client.blockchain.BlockchainStrategy;
import client.client.ClientObject;
import client.commoninterfaces.IPrepareWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.commoninterfaces.IWorkloadObject;
import client.configuration.DistributionConfiguration;
import client.configuration.GeneralConfiguration;
import client.database.Listener;
import client.statistics.*;
import client.supplements.ExceptionHandler;
import client.testworkload.TestPrepareWorkload;
import client.utils.GenericSelectionStrategy;
import client.utils.ListenerUtils;
import client.utils.ThreadPoolFactoryFactoryFacade;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberExecutorScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import co.paralleluniverse.strands.SuspendableCallable;
import com.sun.management.OperatingSystemMXBean;
import corda.workloads.PrepareCordaWorkload;
import fabric.workloads.PrepareFabricWorkload;
import graphene.workloads.PrepareGrapheneWorkload;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.jetbrains.annotations.Nullable;
import quorum.workloads.PrepareQuorumWorkload;
import sawtooth.workloads.PrepareSawtoothWorkload;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientExecutorPool<E> implements IClientPool, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(ClientExecutorPool.class);
    private static final Map<String, List<Integer>> REQUEST_ID_MAP = Collections.synchronizedMap(new LinkedHashMap<>());
    private static final CountDownLatch WAIT_FOR_ALL_CLIENTS_TO_START_LATCH =
            new CountDownLatch(GeneralConfiguration.CLIENT_COUNT);
    private static final AtomicInteger CLIENT_START_COUNTER = new AtomicInteger(GeneralConfiguration.CLIENT_COUNT);
    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = (OperatingSystemMXBean) ManagementFactory
            .getOperatingSystemMXBean();
    private static final int AVAILABLE_PROCESSORS = OPERATING_SYSTEM_MX_BEAN.getAvailableProcessors();
    private static volatile CountDownLatch latchMain;

    public CountDownLatch getLatchClientExecutor() {
        return latchClientExecutor;
    }

    private final CountDownLatch latchClientExecutor;
    private final FiberExecutorScheduler fiberExecutorScheduler;
    private final ThreadPoolExecutor threadExecutorScheduler;
    private final AtomicInteger clientRequestCounter = new AtomicInteger(0);

    public ClientObject getClientObject() {
        return clientObject;
    }

    private final ClientObject clientObject;
    private final GeneralStatisticObject generalStatisticObject;
    private final List<IPrepareWorkload> iPrepareWorkloads =
            new ArrayList<>(GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0));
    private final Queue<IStatistics> statisticList = new ConcurrentLinkedQueue<>();
    private final ClientExecutorStatistics clientExecutorStatistics = new ClientExecutorStatistics();
    private WorkloadPool<?>[] workloadPool;
    private List<E> params;
    private Long clientStart;
    private Long cpuStart;

    public ClientExecutorPool(final ClientObject clientObjectConstructor, final ThreadPoolExecutor schedulerConstructor,
                              final CountDownLatch latchConstructor,
                              final GeneralStatisticObject generalStatisticObjectConstructor) {
        this.threadExecutorScheduler = schedulerConstructor;
        this.fiberExecutorScheduler = null;
        latchMain = latchConstructor;
        this.latchClientExecutor =
                new CountDownLatch(GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                        Collections.singletonList(0), false).get(0));
        this.clientObject = clientObjectConstructor;
        this.generalStatisticObject = generalStatisticObjectConstructor;
    }

    public ClientExecutorPool(final ClientObject clientObjectConstructor,
                              final FiberExecutorScheduler schedulerConstructor,
                              final CountDownLatch latchConstructor,
                              final GeneralStatisticObject generalStatisticObjectConstructor) {
        this.fiberExecutorScheduler = schedulerConstructor;
        this.threadExecutorScheduler = null;
        latchMain = latchConstructor;
        this.latchClientExecutor =
                new CountDownLatch(GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                        Collections.singletonList(0), false).get(0));
        this.clientObject = clientObjectConstructor;
        this.generalStatisticObject = generalStatisticObjectConstructor;
    }

    @Suspendable
    public List<IPrepareWorkload> getIPrepareWorkloads() {
        return iPrepareWorkloads;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E1> void startClient(final E1... params) {

        clientStart = System.nanoTime();
        cpuStart = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
        long clientStartTime = System.currentTimeMillis();
        clientExecutorStatistics.setStartRuntime(clientStart);
        clientExecutorStatistics.setStartTime(clientStartTime);

        try {

            workloadPool = new WorkloadPool[GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS
                    , Collections.singletonList(0), false).get(0)];

            ThreadPoolExecutor executorClient = new ThreadPoolExecutor(
                    GeneralConfiguration.CLIENT_CORE_POOL_SIZE,
                    GeneralConfiguration.CLIENT_MAXIMUM_POOL_SIZE,
                    GeneralConfiguration.CLIENT_POOL_TIMEOUT,
                    GeneralConfiguration.CLIENT_POOL_TIME_UNIT,
                    (BlockingQueue<Runnable>) GeneralConfiguration.CLIENT_ABSTRACT_QUEUE.getDeclaredConstructor().newInstance()
            );

            executorClient.setThreadFactory(ThreadPoolFactoryFactoryFacade.setThreadPoolFactoryWithName("workload" +
                    "-executor-pool-%d-" + clientObject.getClientId()));

            if (fiberExecutorScheduler != null) {
                Strand fiber = new Fiber<>(this.fiberExecutorScheduler
                        , (SuspendableCallable<Void>) () -> executeWorkloadPool(executorClient));
                fiber.setName(clientObject.getClientId() + "-client-fiber");
                fiber.setUncaughtExceptionHandler((f, ex) -> ExceptionHandler.logException(ex));
                fiber.start();
            } else if (threadExecutorScheduler != null) {
                Thread thread = new Thread(() -> executeWorkloadPool(executorClient));
                thread.setName(clientObject.getClientId() + "-client-thread");
                thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                Strand runnable = Strand.of(thread);
                threadExecutorScheduler.execute((Runnable) runnable.getUnderlying());
            }
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    @Nullable
    private Void executeWorkloadPool(final ThreadPoolExecutor executorClient) {

        IPrepareWorkload iPrepareWorkload = prepareBlockchain();

        FiberExecutorScheduler scheduler =
                new FiberExecutorScheduler(GeneralConfiguration.FIBER_WORKLOAD_POOL_NAME,
                        executorClient);

        if (DistributionConfiguration.BEFORE_WAIT_FOR_CLIENTS) {
            handleRequestDistribution();
        }

        if (GeneralConfiguration.CLIENT_RAMP_UP_TIME_BEFORE_PREPARATION > 0) {
            try {
                Strand.sleep(GeneralConfiguration.CLIENT_RAMP_UP_TIME_BEFORE_PREPARATION);
            } catch (SuspendExecution | InterruptedException ex) {
                ExceptionHandler.logException(ex);
            }
        }

        if (GeneralConfiguration.WAIT_FOR_ALL_CLIENTS_TO_START) {
            WAIT_FOR_ALL_CLIENTS_TO_START_LATCH.countDown();
            LOG.info("Decrementing client start counter " + CLIENT_START_COUNTER.decrementAndGet());
            try {
                boolean await = WAIT_FOR_ALL_CLIENTS_TO_START_LATCH.await(GeneralConfiguration.TIMEOUT_LISTENER,
                        GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);
                if (!await) {
                    LOG.error("Await did not complete correctly, wait");
                    System.exit(0);
                }
            } catch (InterruptedException ex) {
                ExceptionHandler.logException(ex);
                LOG.error("Await did not complete correctly, wait");
                System.exit(0);
            }
        }

        if (GeneralConfiguration.DISTRIBUTED_CLIENT_HANDLING) {

            CompletableFuture<Boolean> numberOfClientsRegisteredFuture = null;
            if (GeneralConfiguration.DISTRIBUTED_CLIENT_HANDLING_WAIT_FOR_CLIENT_THRESHOLD) {
                numberOfClientsRegisteredFuture = new CompletableFuture<>();
                Listener listener = new Listener();

                if (GeneralConfiguration.LISTENER_AS_THREAD) {
                    CompletableFuture<Boolean> finalNumberOfClientsRegisteredFuture = numberOfClientsRegisteredFuture;
                    Thread thread = new Thread(() -> {

                        if (GeneralConfiguration.START_DATABASE_LISTENER_ASYNC) {
                            listener.startAsyncListener(finalNumberOfClientsRegisteredFuture);
                        } else {
                            listener.startListenerSync(finalNumberOfClientsRegisteredFuture);
                        }
                    }
                    );
                    thread.setName(clientObject.getClientId() + "-database-listener-thread");
                    thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    thread.start();
                } else {
                    CompletableFuture<Boolean> finalNumberOfClientsRegisteredFuture = numberOfClientsRegisteredFuture;
                    Fiber<Void> fiber =
                            new Fiber<>(() -> {
                                if (GeneralConfiguration.START_DATABASE_LISTENER_ASYNC) {
                                    listener.startAsyncListener(finalNumberOfClientsRegisteredFuture);
                                } else {
                                    listener.startListenerSync(finalNumberOfClientsRegisteredFuture);
                                }
                            });
                    fiber.setName(clientObject.getClientId() + "-database-listener-fiber");
                    fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                    fiber.start();
                }

                try {
                    listener.getIsSubscribed().get(GeneralConfiguration.TIMEOUT_LISTENER,
                            GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }

            }

            try (java.sql.Connection connection = client.database.Connection.getConnection()){

                DateTimeFormatter formatter =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ").withZone(ZoneId.of(GeneralConfiguration.DEFAULT_TIME_ZONE));
                int initialValue = 1;
                String dateFormatted = formatter.format(Instant.ofEpochMilli(System.currentTimeMillis()));
                String query = "INSERT INTO clientcoordination AS cc (current_client_counter, run_id, date)" +
                        "VALUES (?, ?" +
                        ", to_timestamp(?, 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') at time zone'" + GeneralConfiguration.TIME_OFFSET + "')" +
                        "ON CONFLICT (run_id) DO UPDATE " +
                        "SET current_client_counter = cc.current_client_counter + " + initialValue;

                PreparedStatement preparedStatement = connection.prepareStatement(query);
                preparedStatement.setInt(1, initialValue);
                preparedStatement.setString(2, GeneralConfiguration.RUN_ID);
                preparedStatement.setString(3, dateFormatted);

                preparedStatement.executeUpdate();

                preparedStatement.close();

            } catch (SQLException ex) {
                ExceptionHandler.logException(ex);
            }

            if (GeneralConfiguration.DISTRIBUTED_CLIENT_HANDLING_WAIT_FOR_CLIENT_THRESHOLD) {
                try {
                    numberOfClientsRegisteredFuture.get(GeneralConfiguration.TIMEOUT_LISTENER,
                            GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);
                } catch (InterruptedException | ExecutionException | TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                }
            }

        }

        if (DistributionConfiguration.AFTER_WAIT_FOR_CLIENTS) {
            handleRequestDistribution();
        }

        if (GeneralConfiguration.CLIENT_RAMP_UP_TIME_AFTER_PREPARATION > 0) {
            try {
                Strand.sleep(GeneralConfiguration.CLIENT_RAMP_UP_TIME_AFTER_PREPARATION);
            } catch (SuspendExecution | InterruptedException ex) {
                ExceptionHandler.logException(ex);
            }
        }

        LOG.info("Started client: " + clientObject.getClientId());

        for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                Collections.singletonList(0), false).get(0); i++) {
            if (GeneralConfiguration.WORKLOAD_POOL_IS_THREAD) {
                getWorkloadPool()[i] = new WorkloadPool<>(clientObject, executorClient, i,
                        latchClientExecutor, params, generalStatisticObject);
            } else {
                getWorkloadPool()[i] = new WorkloadPool<>(clientObject, scheduler, i,
                        latchClientExecutor,
                        params, generalStatisticObject);
            }
        }

        for (int i = 0; i < GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
          Collections.singletonList(0), false).get(0); i++) {
        //IntStream.range(0, GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
        //        Collections.singletonList(0), false).get(0)).parallel().forEach(i -> {
            LOG.debug("Starting pool: " + i + " of client " + clientObject.getClientId());
            if (GeneralConfiguration.GENERAL_MEASUREMENT_POINT_START == GeneralMeasurementPoint.GeneralMeasurementPointStart.PREPARE_EXECUTE_WORKLOAD) {
                generalStatisticObject.setStartTime(System.nanoTime(), false);
                generalStatisticObject.setCurrentTimeStart(System.currentTimeMillis(), false);
            }
            if (DistributionConfiguration.AFTER_WAIT_FOR_CLIENTS_AFTER_MEASUREMENT_POINT) {
                handleRequestDistribution();
            }
            getWorkloadPool()[i].execute();
        }//);

        iPrepareWorkload.endPrepareWorkload(clientObject);

        statisticList.addAll(iPrepareWorkload.getStatistics());

        LOG.debug("Is fiber ClientExecutorPool: " + (Strand.currentStrand().isFiber() ? Fiber.currentFiber().isFiber() :
                Strand.of(Thread.currentThread()).isFiber()));

        latchMain.countDown();
        return null;
    }

    @Suspendable
    private IPrepareWorkload prepareBlockchain() {
        BlockchainFramework blockchainFramework = BlockchainStrategy.getBlockchainFramework();
        IPrepareWorkload iPrepareWorkload = null;
        try {
            iPrepareWorkload = GenericSelectionStrategy.selectFixed(GeneralConfiguration.I_PREPARE_WORKLOADS,
                    Collections.singletonList(0), false).get(0).
                    getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }
        if (blockchainFramework.equals(BlockchainFramework.Corda)) {
            PrepareCordaWorkload prepareWorkload = (PrepareCordaWorkload) iPrepareWorkload;
            prepareWorkload.prepareWorkload(clientObject);
            params = prepareWorkload.getParams();
            return prepareWorkload;
        } else if (blockchainFramework.equals(BlockchainFramework.Quorum)) {
            PrepareQuorumWorkload prepareWorkload = (PrepareQuorumWorkload) iPrepareWorkload;
            prepareWorkload.prepareWorkload(clientObject);
            params = prepareWorkload.getParams();
            return prepareWorkload;
        } else if (blockchainFramework.equals(BlockchainFramework.HyperledgerFabric)) {
            PrepareFabricWorkload prepareWorkload = (PrepareFabricWorkload) iPrepareWorkload;
            prepareWorkload.prepareWorkload(clientObject);
            params = prepareWorkload.getParams();
            return prepareWorkload;
        } else if (blockchainFramework.equals(BlockchainFramework.HyperledgerSawtooth)) {
            PrepareSawtoothWorkload prepareWorkload = (PrepareSawtoothWorkload) iPrepareWorkload;
            prepareWorkload.prepareWorkload(clientObject);
            params = prepareWorkload.getParams();
            return prepareWorkload;
        } else if (blockchainFramework.equals(BlockchainFramework.Graphene)) {
            PrepareGrapheneWorkload prepareWorkload = (PrepareGrapheneWorkload) iPrepareWorkload;
            prepareWorkload.prepareWorkload(clientObject);
            params = prepareWorkload.getParams();
            return prepareWorkload;
        } else if (blockchainFramework.equals(BlockchainFramework.Test)) {
            TestPrepareWorkload prepareWorkload = (TestPrepareWorkload) iPrepareWorkload;
            prepareWorkload.prepareWorkload(clientObject);
            return prepareWorkload;
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }
    }

    @Suspendable
    public WorkloadPool<?>[] getWorkloadPool() {
        return workloadPool;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E1> void handleRequestDistribution(final E1... params) {

    }

    @Suspendable
    public Queue<IStatistics> getStatisticList() {
        return statisticList;
    }

    @Suspendable
    public synchronized void latch(final WorkloadPool<?>[] workloadPools) {

        if(params != null && params.get(0) != null) {
            ListenerUtils.awaitEndOfExecution((IWorkloadObject) params.get(0), statisticList);
            ListenerUtils.disconnectListeners(clientObject, (IWorkloadObject) params.get(0));
        } else {
            LOG.error("Workload pool null for: " + clientObject.getClientId());
        }

        long cpuEnd = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
        long clientEnd = System.nanoTime();
        long clientEndTime = System.currentTimeMillis();

        handleCpuUsage(clientStart, cpuStart, clientExecutorStatistics, cpuEnd, clientEnd);

        clientExecutorStatistics.setClientObject(clientObject);
        clientExecutorStatistics.setEndTime(clientEndTime);
        clientExecutorStatistics.setEndRuntime(clientEnd);

        statisticList.add(clientExecutorStatistics);

        if (GeneralConfiguration.GeneralMeasurementPointEnd == GeneralMeasurementPoint.GeneralMeasurementPointEnd.LATCH_HANDLING) {
            generalStatisticObject.setEndTime(System.nanoTime(), false);
            generalStatisticObject.setCurrentTimeEnd(System.currentTimeMillis(), false);
        }

        LOG.info("Done workload pool");

        List<Integer> requestNumberList = null;

        for (final WorkloadPool<?> workloadPool : workloadPools) {
            if(workloadPool == null) {
                LOG.error("Workload pool is null");
                continue;
            }
            synchronized (REQUEST_ID_MAP) {
                if (REQUEST_ID_MAP.containsKey(clientObject.getClientId()) && requestNumberList != null) {
                    int i = clientRequestCounter.addAndGet(workloadPool.getRequestCounter().get());
                    requestNumberList.add(i);
                } else {
                    requestNumberList = new ArrayList<>();
                    requestNumberList.add(clientRequestCounter.addAndGet(workloadPool.getRequestCounter().get()));
                }
                REQUEST_ID_MAP.put(clientObject.getClientId(), requestNumberList);
            }

            statisticList.addAll(workloadPool.getStatisticList());
        }

        handleStatistics(clientExecutorStatistics);

        LOG.info("Size of pool: " + workloadPools.length);
        LOG.info("Workloads executed: " + clientRequestCounter.get());

    }

    @Suspendable
    private static void handleCpuUsage(final long completeStart, final long cpuStart,
                                       final ClientExecutorStatistics clientExecutorStatistics, final long cpuEnd,
                                       final long completeEnd) {
        float averageCpuPercent;
        float totalCpuPercent;
        if (completeEnd > completeStart) {
            averageCpuPercent = ((cpuEnd - cpuStart) * 100L) / (float) AVAILABLE_PROCESSORS /
                    (completeEnd - completeStart);
            totalCpuPercent = ((float) (cpuEnd - cpuStart) * 100L) /
                    (completeEnd - completeStart);
        } else {
            averageCpuPercent = 0;
            totalCpuPercent = 0;
        }

        LOG.info("Per core CPU usage (client): " + averageCpuPercent + "%" + " total CPU usage: " + totalCpuPercent + "%");

        clientExecutorStatistics.setTotalCpuUsage(totalCpuPercent);
        clientExecutorStatistics.setCpuUsage(averageCpuPercent);
    }

    @Suspendable
    private void handleStatistics(final ClientExecutorStatistics clientExecutorStatistics) {
        int readCounter = 0;
        int writeCounter = 0;
        int failedReadCounter = 0;
        int failedWriteCounter = 0;
        boolean excludeFailedReadRequests = GeneralConfiguration.EXCLUDE_FAILED_READ_REQUESTS;
        boolean excludeFailedWriteRequests = GeneralConfiguration.EXCLUDE_FAILED_WRITE_REQUESTS;

        for (final WorkloadPool<?> workloadPool : workloadPool) {
            if(workloadPool == null) {
                LOG.error("Skipping workload pool during statistic handling");
                continue;
            }
            for (final IStatistics iStatistics : workloadPool.getStatisticList()) {
                if (iStatistics.getStatisticType() == IStatistics.StatisticType.Read && iStatistics instanceof ReadStatisticObject) {
                    ReadStatisticObject readStatisticObject = (ReadStatisticObject) iStatistics;
                    if (excludeFailedReadRequests && !readStatisticObject.isFailedRequest()) {
                        ++readCounter;
                    } else if (excludeFailedReadRequests && readStatisticObject.isFailedRequest()) {
                        ++failedReadCounter;
                    } else {
                        ++readCounter;
                    }
                }
                if (iStatistics.getStatisticType() == IStatistics.StatisticType.Write && iStatistics instanceof WriteStatisticObject) {
                    WriteStatisticObject writeStatisticObject = (WriteStatisticObject) iStatistics;
                    if (excludeFailedWriteRequests && !writeStatisticObject.isFailedRequest()) {
                        ++writeCounter;
                    } else if (excludeFailedWriteRequests && writeStatisticObject.isFailedRequest()) {
                        ++failedWriteCounter;
                    } else {
                        ++writeCounter;
                    }
                }
            }
        }

        clientExecutorStatistics.setFailedTotal(failedReadCounter + failedWriteCounter);
        clientExecutorStatistics.setExcludeFailedReadRequests(excludeFailedReadRequests);
        clientExecutorStatistics.setExcludeFailedWriteRequests(excludeFailedWriteRequests);
        clientExecutorStatistics.setWriteRequests(writeCounter + failedWriteCounter);
        clientExecutorStatistics.setReadRequests(readCounter + failedReadCounter);
        clientExecutorStatistics.setSuccessfulTotal(readCounter + writeCounter);
        clientExecutorStatistics.setTotalNumberOfRequests(writeCounter + failedWriteCounter + readCounter + failedReadCounter);

    }

    @Suspendable
    public AtomicInteger getClientRequests() {
        return clientRequestCounter;
    }

}
