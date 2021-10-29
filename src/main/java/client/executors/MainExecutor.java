package client.executors;

import client.blockchain.BlockchainStrategy;
import client.client.ClientObject;
import client.client.ClientRegistry;
import client.client.ClientRole;
import client.configuration.GeneralConfiguration;
import client.statistics.*;
import client.supplements.ExceptionHandler;
import client.utils.RequestDistribution;
import client.utils.ResourceMonitor;
import client.utils.ThreadPoolFactoryFactoryFacade;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberExecutorScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static client.configuration.GeneralConfiguration.CLIENT_COUNT;

public class MainExecutor implements IMainExecutor {

    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = (OperatingSystemMXBean) ManagementFactory
            .getOperatingSystemMXBean();
    private static final int AVAILABLE_PROCESSORS = OPERATING_SYSTEM_MX_BEAN.getAvailableProcessors();
    private static final Logger LOG = Logger.getLogger(MainExecutor.class);
    private static final CountDownLatch LATCH_MAIN = new CountDownLatch(GeneralConfiguration.CLIENT_COUNT);
    private static final AtomicInteger COMPLETE_REQUEST_COUNTER = new AtomicInteger(0);

    private MainExecutor() {
    }

    @Suspendable
    public static void main(final String... args) {
        new MainExecutor().startExecutor();
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> void startExecutor(final E... params) {

        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";

        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        prepareExecutor();

        long completeStart = System.nanoTime();
        long cpuStart = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
        long completeStartTime = System.currentTimeMillis();

        ResourceMonitor resourceMonitor =
                new ResourceMonitor();
        monitorResources(resourceMonitor);

        GeneralStatisticObject generalStatisticObject = new GeneralStatisticObject();
        if (GeneralConfiguration.GENERAL_MEASUREMENT_POINT_START == GeneralMeasurementPoint.GeneralMeasurementPointStart.START_OF_APPLICATION) {
            generalStatisticObject.setStartTime(System.nanoTime(), false);
            generalStatisticObject.setCurrentTimeStart(System.currentTimeMillis(), false);
        }

        try {

            ClientExecutorPool<?>[] clientExecutorPool = new ClientExecutorPool[CLIENT_COUNT];

            ThreadPoolExecutor executor = new ThreadPoolExecutor(
                    GeneralConfiguration.MAIN_CORE_POOL_SIZE,
                    GeneralConfiguration.MAIN_MAXIMUM_POOL_SIZE,
                    GeneralConfiguration.MAIN_POOL_TIMEOUT,
                    GeneralConfiguration.MAIN_POOL_TIME_UNIT,
                    (BlockingQueue<Runnable>) GeneralConfiguration.MAIN_ABSTRACT_QUEUE.getDeclaredConstructor().newInstance()
            );

            FiberExecutorScheduler schedulerMain = null;
            if (!GeneralConfiguration.CLIENT_EXECUTOR_POOL_IS_THREAD) {
                executor.setThreadFactory(ThreadPoolFactoryFactoryFacade.setThreadPoolFactoryWithName("client" +
                        "-executor-pool-%d"));
                schedulerMain = new FiberExecutorScheduler(GeneralConfiguration.FIBER_CLIENT_EXECUTOR_POOL_NAME,
                        executor);
            }

            ClientObject[] clientObjects = new ClientObject[CLIENT_COUNT];

            FiberExecutorScheduler finalSchedulerMain = schedulerMain;
            for (int i = 0; i < CLIENT_COUNT; i++) {
            //IntStream.range(0, CLIENT_COUNT).parallel().forEach(i -> {

                LOG.trace("Starting client " + i);

                clientObjects[i] = new ClientObject(GeneralConfiguration.RUN_ID + "-" + GeneralConfiguration.HOST_ID + "-" + GeneralConfiguration.CLIENT_PREFIX, i, 0, 0,
                        ClientRole.GENERAL);

                ClientRegistry.getClientObjects().add(clientObjects[i]);

                if (GeneralConfiguration.CLIENT_EXECUTOR_POOL_IS_THREAD) {
                    clientExecutorPool[i] = new ClientExecutorPool<>(clientObjects[i], executor, LATCH_MAIN,
                            generalStatisticObject);
                } else {
                    clientExecutorPool[i] = new ClientExecutorPool<>(clientObjects[i], finalSchedulerMain, LATCH_MAIN,
                            generalStatisticObject);
                }

                if (GeneralConfiguration.GENERAL_MEASUREMENT_POINT_START == GeneralMeasurementPoint.GeneralMeasurementPointStart.START_CLIENT) {
                    generalStatisticObject.setStartTime(System.nanoTime(), false);
                    generalStatisticObject.setCurrentTimeStart(System.currentTimeMillis(), false);
                }
                clientExecutorPool[i].startClient();
            }//);

            for (int i = 0; i < CLIENT_COUNT; i++) {
            //IntStream.range(0, CLIENT_COUNT).parallel().forEach(i -> {
                LOG.debug("Setting latch for: " + clientObjects[i].getClientId());

                try {
                    boolean await =
                            clientExecutorPool[i].getLatchClientExecutor().await(GeneralConfiguration.TIMEOUT_LISTENER, GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);
                    if(!await) {
                        LOG.error("Await did not complete correctly, clientLatch");
                        //System.exit(0);
                    }
                } catch (InterruptedException ex) {
                    ExceptionHandler.logException(ex);
                    LOG.error("Await did not complete correctly, clientLatch");
                    //System.exit(0);
                }

                clientExecutorPool[i].latch(clientExecutorPool[i].getWorkloadPool());
                LOG.debug("Set latch for: " + clientObjects[i].getClientId());
            }//);

            try {
                boolean await = LATCH_MAIN.await(GeneralConfiguration.TIMEOUT_LISTENER,
                        GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);
                if(!await) {
                    LOG.error("Await did not complete correctly, latch");
                    System.exit(0);
                }
            } catch (InterruptedException ex) {
                ExceptionHandler.logException(ex);
                LOG.error("Await did not complete correctly, latch");
                System.exit(0);
            }

            if (GeneralConfiguration.GeneralMeasurementPointEnd == GeneralMeasurementPoint.GeneralMeasurementPointEnd.END_OF_APPLICATION) {
                generalStatisticObject.setEndTime(System.nanoTime(), false);
                generalStatisticObject.setCurrentTimeEnd(System.currentTimeMillis(), false);
            }

            handleStatistics(generalStatisticObject, clientExecutorPool);

            LOG.info("Executed workloads in total: " + COMPLETE_REQUEST_COUNTER.get());
            LOG.info("Done main");

            ResourceMonitor.getIsDone().set(true);

            try {
                ResourceMonitor.getFinishedList().get(GeneralConfiguration.TIMEOUT_LISTENER,
                        GeneralConfiguration.TIMEOUT_LISTENER_TIME_UNIT);
            } catch (ExecutionException | TimeoutException ex) {
                ExceptionHandler.logException(ex);
            }

            for (final SystemStatistics systemStatistics : ResourceMonitor.getSystemStatisticsList()) {
                systemStatistics.writeStatistics();
            }

            long cpuEnd = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
            long completeEnd = System.nanoTime();
            long completeEndTime = System.currentTimeMillis();

            handleCpuUsage(completeStart, cpuStart, generalStatisticObject, cpuEnd, completeEnd);

            generalStatisticObject.setCompleteStartTime(completeStartTime);
            generalStatisticObject.setCompleteEndTime(completeEndTime);
            generalStatisticObject.setCompleteStartRuntime(completeStart);
            generalStatisticObject.setCompleteEndRuntime(completeEnd);

            RequestDistribution.writeValueMap();

            generalStatisticObject.writeStatistics();

            if(GeneralConfiguration.WRITE_TO_DATABASE) {
                PrepareStatementCollection.getPreparedStatementMap().forEach(
                        (q, i) -> {
                            try {
                                i.getLeft().executeBatch();
                                i.getRight().commit();
                                i.getRight().close();
                            } catch (SQLException ex) {
                                ExceptionHandler.logException(ex);
                            } catch (ArrayIndexOutOfBoundsException ex) {
                                ExceptionHandler.logException(ex);
                                LOG.error("IndexOutOfBounds - affected query: " + q);
                            }
                        }
                );
            }

            if (GeneralConfiguration.CLIENT_RAMP_DOWN_TIME_AFTER_EXECUTION > 0) {
                Strand.sleep(GeneralConfiguration.CLIENT_RAMP_DOWN_TIME_AFTER_EXECUTION);
            }

            endExecutor();

            System.exit(0);
        } catch (SuspendExecution | InterruptedException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    private void endExecutor() {
    }

    @Suspendable
    private void prepareExecutor() {

        switch (GeneralConfiguration.BLOCKCHAIN_FRAMEWORK) {
            case Graphene:
                graphene.helper.Helper.readServerMapFromFile(graphene.configuration.Configuration.DEFAULT_KEY_FILE,
                        graphene.configuration.Configuration.START_ACCOUNT_ID_PREFIX,
                        graphene.configuration.Configuration.START_ACCOUNT_ID);
                return;
            case Quorum:
                quorum.helper.Helper.readToIpMapFromFile(quorum.configuration.Configuration.ADDRESS_OR_WALLET_FILE);
                return;
            default:
                LOG.debug("No action taken for preparation");
        }

    }

    @Suspendable
    private static void handleCpuUsage(final long completeStart, final long cpuStart,
                                       final GeneralStatisticObject generalStatisticObject, final long cpuEnd,
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

        LOG.info("Per core CPU usage (total): " + averageCpuPercent + "%" + " total CPU usage: " + totalCpuPercent +
                "%");

        generalStatisticObject.setTotalCpuUsage(totalCpuPercent);
        generalStatisticObject.setCpuUsage(averageCpuPercent);
    }

    @Suspendable
    private static void handleStatistics(final GeneralStatisticObject generalStatisticObject,
                                         final ClientExecutorPool<?>[] clientExecutorPool) {
        int readCounter = 0;
        int writeCounter = 0;
        int failedReadCounter = 0;
        int failedWriteCounter = 0;
        boolean excludeFailedReadRequests = GeneralConfiguration.EXCLUDE_FAILED_READ_REQUESTS;
        boolean excludeFailedWriteRequests = GeneralConfiguration.EXCLUDE_FAILED_WRITE_REQUESTS;

        ImmutableTriple<List<Double>, IStatistics.StatisticType, String> writeLatencyTriple =
                ImmutableTriple.of(new ArrayList<>(), IStatistics.StatisticType.Write,
                        BlockchainStrategy.getBlockchainFrameworkAsString());
        ImmutableTriple<List<Double>, IStatistics.StatisticType, String> readLatencyTriple =
                ImmutableTriple.of(new ArrayList<>(), IStatistics.StatisticType.Read,
                        BlockchainStrategy.getBlockchainFrameworkAsString());
        ImmutableTriple<List<Double>, IStatistics.StatisticType, String> listenerLatencyTriple =
                ImmutableTriple.of(new ArrayList<>(), IStatistics.StatisticType.Listener,
                        BlockchainStrategy.getBlockchainFrameworkAsString());

        for (final ClientExecutorPool<?> clientPool : clientExecutorPool) {
            COMPLETE_REQUEST_COUNTER.set(COMPLETE_REQUEST_COUNTER.addAndGet(clientPool.getClientRequests().get()));

            LOG.debug("Start writing client pool statistics for " + clientPool.getClientObject().getClientId() + "...");

            Queue<IStatistics> statisticList = clientPool.getStatisticList();

            for (final IStatistics iStatistics : statisticList) {
                if (iStatistics.getStatisticType() == IStatistics.StatisticType.Read && iStatistics instanceof ReadStatisticObject) {
                    ReadStatisticObject readStatisticObject = (ReadStatisticObject) iStatistics;
                    if (excludeFailedReadRequests && !readStatisticObject.isFailedRequest()) {
                        ++readCounter;
                        readLatencyTriple.getLeft().add(readStatisticObject.getLatency());
                    } else if (excludeFailedReadRequests && readStatisticObject.isFailedRequest()) {
                        ++failedReadCounter;
                    } else {
                        ++readCounter;
                        readLatencyTriple.getLeft().add(readStatisticObject.getLatency());
                    }
                }
                if (iStatistics.getStatisticType() == IStatistics.StatisticType.Write && iStatistics instanceof WriteStatisticObject) {
                    WriteStatisticObject writeStatisticObject = (WriteStatisticObject) iStatistics;
                    if (excludeFailedWriteRequests && !writeStatisticObject.isFailedRequest()) {
                        ++writeCounter;
                        writeLatencyTriple.getLeft().add(writeStatisticObject.getLatency());
                    } else if (excludeFailedWriteRequests && writeStatisticObject.isFailedRequest()) {
                        ++failedWriteCounter;
                    } else {
                        ++writeCounter;
                        writeLatencyTriple.getLeft().add(writeStatisticObject.getLatency());
                    }
                }
                if (iStatistics.getStatisticType() == IStatistics.StatisticType.Listener && iStatistics instanceof ListenerStatisticObject) {
                    ListenerStatisticObject listenerStatisticObject = (ListenerStatisticObject) iStatistics;
                    listenerLatencyTriple.getLeft().addAll(listenerStatisticObject.getLatencyValues(
                            GeneralConfiguration.EXCLUDE_INVALID_VALUES, GeneralConfiguration.EXCLUDE_ERROR_VALUES,
                            GeneralConfiguration.EXCLUDE_EXISTING_VALUES));
                }
            }

            for (final IStatistics statistic : clientPool.getStatisticList()) {
                LOG.debug("Writing client pool statistics for " + clientPool.getClientObject().getClientId() + "...");
                statistic.writeStatistics();
            }

        }

        DescriptiveStatistics.printStatistics(writeLatencyTriple.getLeft(), writeLatencyTriple.getMiddle(),
                writeLatencyTriple.getRight());
        DescriptiveStatistics.writeStatistics(writeLatencyTriple.getLeft(), writeLatencyTriple.getMiddle(),
                writeLatencyTriple.getRight());

        DescriptiveStatistics.printStatistics(readLatencyTriple.getLeft(), readLatencyTriple.getMiddle(),
                readLatencyTriple.getRight());
        DescriptiveStatistics.writeStatistics(readLatencyTriple.getLeft(), readLatencyTriple.getMiddle(),
                readLatencyTriple.getRight());

        DescriptiveStatistics.printStatistics(listenerLatencyTriple.getLeft(), listenerLatencyTriple.getMiddle(),
                listenerLatencyTriple.getRight());
        DescriptiveStatistics.writeStatistics(listenerLatencyTriple.getLeft(), listenerLatencyTriple.getMiddle(),
                listenerLatencyTriple.getRight());

        generalStatisticObject.setClientCounter(ClientRegistry.getClientObjects().size());
        generalStatisticObject.setFailedTotal(failedReadCounter + failedWriteCounter);
        generalStatisticObject.setExcludeFailedReadRequests(excludeFailedReadRequests);
        generalStatisticObject.setExcludeFailedWriteRequests(excludeFailedWriteRequests);
        generalStatisticObject.setNumberOfWorkloads(COMPLETE_REQUEST_COUNTER.get());
        generalStatisticObject.setWriteRequests(writeCounter + failedWriteCounter);
        generalStatisticObject.setReadRequests(readCounter + failedReadCounter);
        generalStatisticObject.setSuccessfulTotal(readCounter + writeCounter);
        generalStatisticObject.setTotalNumberOfRequests(writeCounter + failedWriteCounter + readCounter + failedReadCounter);
    }

    @Suspendable
    private static void monitorResources(final ResourceMonitor resourceMonitor) {
        if (GeneralConfiguration.ENABLE_RESOURCE_MONITOR) {
            if (GeneralConfiguration.RESOURCE_MONITOR_AS_THREAD) {
                Thread thread = new Thread(resourceMonitor::updateResourceUsage);
                thread.setName("-resource-thread");
                thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                thread.start();
            } else {
                Fiber<Void> fiber = new Fiber<>(resourceMonitor::updateResourceUsage);
                fiber.setName("-resource-fiber");
                fiber.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
                fiber.start();
            }
        }
    }

}
