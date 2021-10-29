package client.utils;

import client.configuration.GeneralConfiguration;
import client.statistics.SystemStatistics;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import com.sun.management.OperatingSystemMXBean;
import org.apache.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResourceMonitor {

    private static final Logger LOG = Logger.getLogger(ResourceMonitor.class);

    private static final OperatingSystemMXBean OS_BEAN = ManagementFactory.getPlatformMXBean(
            OperatingSystemMXBean.class);
    private static final String OS_NAME = OS_BEAN.getName();
    private static final String OS_ARCH = OS_BEAN.getArch();
    private static final String OS_VERSION = OS_BEAN.getVersion();
    private static final int OS_NUMBER_OF_PROCESSORS = OS_BEAN.getAvailableProcessors();
    private static final long OS_TOTAL_MEMORY = OS_BEAN.getTotalPhysicalMemorySize();

    private static final Runtime RUNTIME = Runtime.getRuntime();
    private static final AtomicBoolean IS_DONE = new AtomicBoolean(false);
    private static final Queue<SystemStatistics> SYSTEM_STATISTICS_LIST =
           new ConcurrentLinkedQueue<>();

    @Suspendable
    public static CompletableFuture<Boolean> getFinishedList() {
        return FINISHED_LIST;
    }

    private static final CompletableFuture<Boolean> FINISHED_LIST = new CompletableFuture<Boolean>();

    @Suspendable
    public static AtomicBoolean getIsDone() {
        return IS_DONE;
    }

    @Suspendable
    public static synchronized Queue<SystemStatistics> getSystemStatisticsList() {
        return SYSTEM_STATISTICS_LIST;
    }

    @Suspendable
    public void updateResourceUsage() {

        if (OS_NUMBER_OF_PROCESSORS != RUNTIME.availableProcessors()) {
            LOG.error("Number of processors not equal");
        }

        while (!IS_DONE.get()) {
            SystemStatistics systemStatistics = new SystemStatistics();
            systemStatistics.setOsName(OS_NAME);
            systemStatistics.setOsArch(OS_ARCH);
            systemStatistics.setOsVersion(OS_VERSION);
            systemStatistics.setOsNumberOfCores(OS_NUMBER_OF_PROCESSORS);
            systemStatistics.setAvailableProcessors(RUNTIME.availableProcessors());
            systemStatistics.setOsTotalMemory(OS_TOTAL_MEMORY);
            systemStatistics.setOsFreePhysicalMemorySize(OS_BEAN.getFreePhysicalMemorySize());
            systemStatistics.setOsTotalSwapSpaceSize(OS_BEAN.getTotalSwapSpaceSize());
            systemStatistics.setOsFreeSwapSpaceSize(OS_BEAN.getFreeSwapSpaceSize());
            systemStatistics.setOsCommittedVirtualMemorySize(OS_BEAN.getCommittedVirtualMemorySize());
            systemStatistics.setTotalMemory(RUNTIME.totalMemory());
            systemStatistics.setAvailableMemory(availableMemory());
            systemStatistics.setMaxMemory(RUNTIME.maxMemory());
            systemStatistics.setFreeMemory(RUNTIME.freeMemory());
            systemStatistics.setUsedMemory(usedMemory());
            systemStatistics.setOsSystemCpuLoad(OS_BEAN.getSystemCpuLoad());
            systemStatistics.setOsProcessCpuLoad(OS_BEAN.getProcessCpuLoad());
            systemStatistics.setOsProcessCpuTime(OS_BEAN.getProcessCpuTime());
            systemStatistics.setOsSystemLoadAverage(OS_BEAN.getSystemLoadAverage());

            SYSTEM_STATISTICS_LIST.add(systemStatistics);

            try {
                Strand.sleep(GeneralConfiguration.SYSTEM_STATISTICS_REPOLL_INTERVAL);
            } catch (SuspendExecution | InterruptedException ex) {
                ExceptionHandler.logException(ex);
            }
        }

        FINISHED_LIST.complete(true);

    }

    @Suspendable
    private static long usedMemory() {
        return (ResourceMonitor.RUNTIME.totalMemory() - ResourceMonitor.RUNTIME.freeMemory());
    }

    @Suspendable
    private static long availableMemory() {
        return (ResourceMonitor.RUNTIME.maxMemory()
                - (ResourceMonitor.RUNTIME.totalMemory() - ResourceMonitor.RUNTIME.freeMemory()));
    }

}
