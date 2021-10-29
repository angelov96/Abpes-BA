package client.executors;

import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.configuration.DistributionConfiguration;
import client.configuration.GeneralConfiguration;
import client.statistics.*;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberExecutorScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import co.paralleluniverse.strands.SuspendableRunnable;
import com.google.common.util.concurrent.RateLimiter;
import com.sun.management.OperatingSystemMXBean;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkloadPool<E> implements IWorkloadPool, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(WorkloadPool.class);
    private static final RateLimiter RATE_LIMITER = GeneralConfiguration.ENABLE_RATE_LIMITER_FOR_WORKLOAD ?
            RateLimiter.create(
                    GenericSelectionStrategy.selectFixed(
                            GeneralConfiguration.WORKLOADS_PER_SECOND, Collections.singletonList(0), false).get(0)) :
            null;
    private static final AtomicInteger REQUEST_COUNTER_FOR_ALL_CLIENTS = new AtomicInteger(0);
    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = (OperatingSystemMXBean) ManagementFactory
            .getOperatingSystemMXBean();
    private static final int AVAILABLE_PROCESSORS = OPERATING_SYSTEM_MX_BEAN.getAvailableProcessors();
    private final FiberExecutorScheduler fiberExecutorScheduler;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final GeneralStatisticObject generalStatisticObject;
    private final Queue<IStatistics> statisticList = new ConcurrentLinkedQueue<>();
    private final AtomicInteger requestCounter = new AtomicInteger(0);
    private final CountDownLatch latch;
    private final List<E> paramList;
    private final int workloadNumber;
    private final List<IExecuteWorkload> iExecuteWorkloads =
            new ArrayList<>(GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                    Collections.singletonList(0), false).get(0));
    private final ClientObject clientObject;

    public <EC> WorkloadPool(final ClientObject clientObjectConstructor,
                             final FiberExecutorScheduler fiberExecutorSchedulerConstructor,
                             final int i,
                             final CountDownLatch latchConstructor, final List<EC> paramList,
                             final GeneralStatisticObject generalStatisticObjectConstructor) {
        this.fiberExecutorScheduler = fiberExecutorSchedulerConstructor;
        this.threadPoolExecutor = null;
        this.latch = latchConstructor;
        this.paramList = (List<E>) paramList;
        this.clientObject = clientObjectConstructor;
        this.workloadNumber = i;
        this.generalStatisticObject = generalStatisticObjectConstructor;
    }

    public <EC> WorkloadPool(final ClientObject clientObjectConstructor,
                             final ThreadPoolExecutor threadPoolExecutorConstructor, final int i,
                             final CountDownLatch latchConstructor, final List<EC> paramList,
                             final GeneralStatisticObject generalStatisticObjectConstructor) {
        this.threadPoolExecutor = threadPoolExecutorConstructor;
        this.fiberExecutorScheduler = null;
        this.latch = latchConstructor;
        this.paramList = (List<E>) paramList;
        this.clientObject = clientObjectConstructor;
        this.workloadNumber = i;
        this.generalStatisticObject = generalStatisticObjectConstructor;
    }

    @Suspendable
    public List<IExecuteWorkload> getIExecuteWorkloads() {
        return iExecuteWorkloads;
    }

    @Suspendable
    public ClientObject getClientObject() {
        return clientObject;
    }

    @Suspendable
    public AtomicInteger getRequestCounter() {
        return requestCounter;
    }

    @SafeVarargs
    @Suspendable
    public final <E1> void execute(final E1... params) {

        WorkloadPoolStatistics workloadPoolStatistics = new WorkloadPoolStatistics();
        long workloadStartBeforeRateLimiter = System.nanoTime();
        long cpuStartBeforeRateLimiter = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
        long workloadStartTimeBeforeRateLimiter = System.currentTimeMillis();
        workloadPoolStatistics.setStartRuntimeBeforeRateLimiter(workloadStartBeforeRateLimiter);
        workloadPoolStatistics.setStartTimeBeforeRateLimiter(workloadStartTimeBeforeRateLimiter);

        if (DistributionConfiguration.BEFORE_WORKLOAD_RATE_LIMITER) {
            handleRequestDistribution();
        }

        if (GeneralConfiguration.ENABLE_RATE_LIMITER_FOR_WORKLOAD) {
            RATE_LIMITER.acquire();
        }

        if (DistributionConfiguration.AFTER_WORKLOAD_RATE_LIMITER) {
            handleRequestDistribution();
        }

        long workloadStart = System.nanoTime();
        long cpuStart = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
        long workloadStartTime = System.currentTimeMillis();
        workloadPoolStatistics.setStartRuntime(workloadStart);
        workloadPoolStatistics.setStartTime(workloadStartTime);

        LOG.info("Starting workload " + workloadNumber + " by " + clientObject.getClientId());

        if (fiberExecutorScheduler != null) {
            Strand fiber = new Fiber<>(this.fiberExecutorScheduler
                    , (SuspendableRunnable) () -> executeWorkloadRoutine(workloadPoolStatistics, workloadStart,
                    workloadStartBeforeRateLimiter, cpuStart, cpuStartBeforeRateLimiter));
            fiber.setName(clientObject.getClientId() + "-workload-fiber");
            fiber.setUncaughtExceptionHandler((f, ex) -> ExceptionHandler.logException(ex));
            fiber.start();
        } else if (threadPoolExecutor != null) {
            Thread thread = new Thread(() -> executeWorkloadRoutine(workloadPoolStatistics, workloadStart,
                    workloadStartBeforeRateLimiter, cpuStart, cpuStartBeforeRateLimiter));
            thread.setName(clientObject.getClientId() + "-workload-thread");
            thread.setUncaughtExceptionHandler((t, ex) -> ExceptionHandler.logException(ex));
            Strand runnable = Strand.of(thread);
            threadPoolExecutor.submit((Runnable) runnable.getUnderlying());
        }
    }

    @Suspendable
    private void executeWorkloadRoutine(final WorkloadPoolStatistics workloadPoolStatistics, final long workloadStart
            , final long workloadStartBeforeRateLimiter, final long cpuStart, final long cpuStartBeforeRateLimiter) {

        IExecuteWorkload iExecuteWorkload = null;
        try {
            iExecuteWorkload = GenericSelectionStrategy.selectFixed(
                    GeneralConfiguration.I_EXECUTE_WORKLOADS, Collections.singletonList(0), false).get(0).getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        if (DistributionConfiguration.BEFORE_WORKLOAD_MEASUREMENT) {
            handleRequestDistribution();
        }

        if (GeneralConfiguration.GENERAL_MEASUREMENT_POINT_START == GeneralMeasurementPoint.GeneralMeasurementPointStart.EXECUTE_WORKLOAD) {
            generalStatisticObject.setStartTime(System.nanoTime(), false);
            generalStatisticObject.setCurrentTimeStart(System.currentTimeMillis(), false);
        }

        if (DistributionConfiguration.AFTER_WORKLOAD_MEASUREMENT) {
            handleRequestDistribution();
        }

        if (GeneralConfiguration.CLIENT_RAMP_UP_TIME_BEFORE_WORKLOAD_EXECUTION > 0) {
            try {
                Strand.sleep(GeneralConfiguration.CLIENT_RAMP_UP_TIME_BEFORE_WORKLOAD_EXECUTION);
            } catch (SuspendExecution | InterruptedException ex) {
                ExceptionHandler.logException(ex);
            }
        }

        Objects.requireNonNull(iExecuteWorkload).executeWorkload(paramList, clientObject, workloadNumber);

        if (GeneralConfiguration.DEBUG_POOL) {
            ThreadPoolExecutor threadPoolExecutor;
            if (this.threadPoolExecutor == null) {
                threadPoolExecutor = (ThreadPoolExecutor) Objects.requireNonNull(fiberExecutorScheduler).getExecutor();
                debugPool(threadPoolExecutor, false);
            } else {
                debugPool(this.threadPoolExecutor, true);
            }
        }

        LOG.debug("Is fiber WorkloadPool: " + (Strand.currentStrand().isFiber() ? Fiber.currentFiber().isFiber() :
                Strand.of(Thread.currentThread()).isFiber()));

        Queue<IStatistics> statistics = iExecuteWorkload.getStatistics();
        statisticList.addAll(statistics);

        Objects.requireNonNull(iExecuteWorkload).endWorkload(paramList, clientObject);

        iExecuteWorkloads.add(iExecuteWorkload);

        requestCounter.incrementAndGet();

        String strandName = (Strand.currentStrand().isFiber() ? Fiber.currentFiber().getName() :
                Strand.of(Thread.currentThread()).getName());
        LOG.info(" Executed workload by: "
                + strandName + " Total workloads executed: " + REQUEST_COUNTER_FOR_ALL_CLIENTS.incrementAndGet());

        workloadPoolStatistics.setClientObject(clientObject);
        long cpuEnd = OPERATING_SYSTEM_MX_BEAN.getProcessCpuTime();
        long workloadEnd = System.nanoTime();
        long workloadEndTime = System.currentTimeMillis();

        handleCpuUsage(workloadStart, workloadStartBeforeRateLimiter, cpuStart, cpuStartBeforeRateLimiter,
                workloadPoolStatistics, cpuEnd, workloadEnd);

        workloadPoolStatistics.setEndTime(workloadEndTime);
        workloadPoolStatistics.setEndRuntime(workloadEnd);
        workloadPoolStatistics.setWorkloadId(workloadNumber);

        handleStatistics(workloadPoolStatistics);

        statisticList.add(workloadPoolStatistics);

        latch.countDown();
    }

    @Suspendable
    private static void handleCpuUsage(final long workloadStart, final long workloadStartBeforeRateLimiter,
                                       final long cpuStart, final long cpuStartBeforeRateLimiter,
                                       final WorkloadPoolStatistics workloadPoolStatistics, final long cpuEnd,
                                       final long workloadEnd) {

        float averageCpuPercent;
        float totalCpuPercent;
        float averageCpuPercentBeforeRateLimiter;
        float totalCpuPercentBeforeRateLimiter;
        if (workloadEnd > workloadStart) {
            averageCpuPercent = ((cpuEnd - cpuStart) * 100L) / (float) AVAILABLE_PROCESSORS /
                    (workloadEnd - workloadStart);
            totalCpuPercent = ((float) (cpuEnd - cpuStart) * 100L) /
                    (workloadEnd - workloadStart);
        } else {
            averageCpuPercent = 0;
            totalCpuPercent = 0;
        }

        if (workloadEnd > workloadStartBeforeRateLimiter) {
            averageCpuPercentBeforeRateLimiter =
                    ((cpuEnd - cpuStartBeforeRateLimiter) * 100L) / (float) AVAILABLE_PROCESSORS /
                            (workloadEnd - workloadStartBeforeRateLimiter);
            totalCpuPercentBeforeRateLimiter = ((float) (cpuEnd - cpuStartBeforeRateLimiter) * 100L) /
                    (workloadEnd - workloadStartBeforeRateLimiter);
        } else {
            averageCpuPercentBeforeRateLimiter = 0;
            totalCpuPercentBeforeRateLimiter = 0;
        }

        LOG.info("Per core CPU usage (workload before rate limiter): " + averageCpuPercentBeforeRateLimiter + "%" +
                " total CPU usage: " + totalCpuPercentBeforeRateLimiter + "%");
        LOG.info("Per core CPU usage (workload): " + averageCpuPercent + "%" + " total CPU usage: " + totalCpuPercent + "%");

        workloadPoolStatistics.setTotalCpuUsageBeforeRateLimiter(totalCpuPercentBeforeRateLimiter);
        workloadPoolStatistics.setCpuUsageBeforeRateLimiter(averageCpuPercentBeforeRateLimiter);
        workloadPoolStatistics.setTotalCpuUsage(totalCpuPercent);
        workloadPoolStatistics.setCpuUsage(averageCpuPercent);
    }

    @Suspendable
    private void debugPool(final ThreadPoolExecutor threadPoolExecutor, final boolean isThread) {
        if (isThread) {
            LOG.info("Thread name: " + Thread.currentThread().getName() + " Completed tasks " + threadPoolExecutor.getCompletedTaskCount() +
                    " Task count " + threadPoolExecutor.getTaskCount() + " Active count " + threadPoolExecutor.getActiveCount());
        } else {
            LOG.info("Fiber name: " + Fiber.currentFiber().getName() + " Completed tasks " + threadPoolExecutor.getCompletedTaskCount() +
                    " Task count " + threadPoolExecutor.getTaskCount() + " Active count " + threadPoolExecutor.getActiveCount());
        }
    }

    @Suspendable
    private void handleStatistics(final WorkloadPoolStatistics workloadPoolStatistics) {
        int readCounter = 0;
        int writeCounter = 0;
        int failedReadCounter = 0;
        int failedWriteCounter = 0;
        boolean excludeFailedReadRequests = GeneralConfiguration.EXCLUDE_FAILED_READ_REQUESTS;
        boolean excludeFailedWriteRequests = GeneralConfiguration.EXCLUDE_FAILED_WRITE_REQUESTS;

        Queue<IStatistics> statisticList = getStatisticList();

        for (final IStatistics iStatistics : statisticList) {
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

        workloadPoolStatistics.setFailedTotal(failedReadCounter + failedWriteCounter);
        workloadPoolStatistics.setExcludeFailedReadRequests(excludeFailedReadRequests);
        workloadPoolStatistics.setExcludeFailedWriteRequests(excludeFailedWriteRequests);
        workloadPoolStatistics.setWriteRequests(writeCounter + failedWriteCounter);
        workloadPoolStatistics.setReadRequests(readCounter + failedReadCounter);
        workloadPoolStatistics.setSuccessfulTotal(readCounter + writeCounter);
        workloadPoolStatistics.setTotalNumberOfRequests(writeCounter + failedWriteCounter + readCounter + failedReadCounter);

    }

    @Suspendable
    public Queue<IStatistics> getStatisticList() {
        return statisticList;
    }

    @SafeVarargs
    @Override
    @Suspendable
    public final <E1> void handleRequestDistribution(final E1... params) {
    }

}
