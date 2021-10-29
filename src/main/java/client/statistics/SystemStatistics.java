package client.statistics;

import client.blockchain.BlockchainStrategy;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class SystemStatistics implements IStatistics {
    private static final Logger LOG = Logger.getLogger(SystemStatistics.class);
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.00");
    private static final String[] HEADER = new String[]{
            "OS_NAME",
            "OS_ARCH",
            "OS_VERSION",
            "OS_NUMBER_OF_CORES",
            "JVM_AVAILABLE_PROCESSORS",
            "OS_TOTAL_MEMORY",
            "OS_FREE_PHYSICAL_MEMORY_SIZE",
            "OS_TOTAL_SWAP_SPACE_SIZE",
            "OS_FREE_SWAP_SPACE_SIZE",
            "OS_COMMITTED_VIRTUAL_MEMORY_SIZE",
            "JVM_TOTAL_MEMORY",
            "JVM_AVAILABLE_MEMORY",
            "JVM_MAX_MEMORY",
            "JVM_FREE_MEMORY",
            "JVM_USED_MEMORY",
            "OS_SYSTEM_CPU_LOAD",
            "OS_PROCESS_CPU_LOAD",
            "OS_PROCESS_CPU_TIME",
            "OS_SYSTEM_LOAD_AVERAGE",
            "CUMULATIVE_COUNT",
            "BASIC_SYSTEM",
            "STATISTIC_TYPE",
            "CID"
    };
    private static final AtomicInteger CUMULATIVE_COUNT = new AtomicInteger(0);
    private String osName;
    private String osArch;
    private String osVersion;
    private int osNumberOfCores;
    private long osTotalMemory;
    private long totalMemory;
    private long freeMemory;
    private long maxMemory;
    private int availableProcessors;
    private long availableMemory;
    private long usedMemory;
    private long osCommittedVirtualMemorySize;
    private long osFreeSwapSpaceSize;
    private long osTotalSwapSpaceSize;
    private long osFreePhysicalMemorySize;
    private double osSystemCpuLoad;
    private double osProcessCpuLoad;
    private double osProcessCpuTime;
    private double osSystemLoadAverage;

    public String getOsName() {
        return osName;
    }

    public void setOsName(final String osName) {
        this.osName = osName;
    }

    public String getOsArch() {
        return osArch;
    }

    public void setOsArch(final String osArch) {
        this.osArch = osArch;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(final String osVersion) {
        this.osVersion = osVersion;
    }

    public int getOsNumberOfCores() {
        return osNumberOfCores;
    }

    public void setOsNumberOfCores(final int osNumberOfCores) {
        this.osNumberOfCores = osNumberOfCores;
    }

    public long getOsTotalMemory() {
        return osTotalMemory;
    }

    public void setOsTotalMemory(final long osTotalMemory) {
        this.osTotalMemory = osTotalMemory;
    }

    public long getTotalMemory() {
        return totalMemory;
    }

    public void setTotalMemory(final long totalMemory) {
        this.totalMemory = totalMemory;
    }

    public long getFreeMemory() {
        return freeMemory;
    }

    public void setFreeMemory(final long freeMemory) {
        this.freeMemory = freeMemory;
    }

    public long getMaxMemory() {
        return maxMemory;
    }

    public void setMaxMemory(final long maxMemory) {
        this.maxMemory = maxMemory;
    }

    public int getAvailableProcessors() {
        return availableProcessors;
    }

    public void setAvailableProcessors(final int availableProcessors) {
        this.availableProcessors = availableProcessors;
    }

    public long getAvailableMemory() {
        return availableMemory;
    }

    public void setAvailableMemory(final long availableMemory) {
        this.availableMemory = availableMemory;
    }

    public long getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(final long usedMemory) {
        this.usedMemory = usedMemory;
    }

    public long getOsCommittedVirtualMemorySize() {
        return osCommittedVirtualMemorySize;
    }

    public void setOsCommittedVirtualMemorySize(final long osCommittedVirtualMemorySize) {
        this.osCommittedVirtualMemorySize = osCommittedVirtualMemorySize;
    }

    public long getOsFreeSwapSpaceSize() {
        return osFreeSwapSpaceSize;
    }

    public void setOsFreeSwapSpaceSize(final long osFreeSwapSpaceSize) {
        this.osFreeSwapSpaceSize = osFreeSwapSpaceSize;
    }

    public long getOsTotalSwapSpaceSize() {
        return osTotalSwapSpaceSize;
    }

    public void setOsTotalSwapSpaceSize(final long osTotalSwapSpaceSize) {
        this.osTotalSwapSpaceSize = osTotalSwapSpaceSize;
    }

    public long getOsFreePhysicalMemorySize() {
        return osFreePhysicalMemorySize;
    }

    public void setOsFreePhysicalMemorySize(final long osFreePhysicalMemorySize) {
        this.osFreePhysicalMemorySize = osFreePhysicalMemorySize;
    }

    public double getOsSystemCpuLoad() {
        return osSystemCpuLoad;
    }

    public void setOsSystemCpuLoad(final double osSystemCpuLoad) {
        this.osSystemCpuLoad = osSystemCpuLoad;
    }

    public double getOsProcessCpuLoad() {
        return osProcessCpuLoad;
    }

    public void setOsProcessCpuLoad(final double osProcessCpuLoad) {
        this.osProcessCpuLoad = osProcessCpuLoad;
    }

    public double getOsProcessCpuTime() {
        return osProcessCpuTime;
    }

    public void setOsProcessCpuTime(final double osProcessCpuTime) {
        this.osProcessCpuTime = osProcessCpuTime;
    }

    public double getOsSystemLoadAverage() {
        return osSystemLoadAverage;
    }

    public void setOsSystemLoadAverage(final double osSystemLoadAverage) {
        this.osSystemLoadAverage = osSystemLoadAverage;
    }

    @SafeVarargs
    @Override
    public final <E> void writeStatistics(final E... params) {
        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        int cumulativeCount = CUMULATIVE_COUNT.incrementAndGet();

        if (GeneralConfiguration.WRITE_TO_DATABASE) {
            writeToDatabase(cumulativeCount);
        }

        List<Object> valuesToWrite = new ArrayList<>();

        valuesToWrite.add(osName);
        valuesToWrite.add(osArch);
        valuesToWrite.add(osVersion);
        valuesToWrite.add(osNumberOfCores);
        valuesToWrite.add(availableProcessors);
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(osTotalMemory));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(osFreePhysicalMemorySize));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(osTotalSwapSpaceSize));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(osFreeSwapSpaceSize));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(osCommittedVirtualMemorySize));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(totalMemory));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(availableMemory));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(maxMemory));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(freeMemory));
        valuesToWrite.add(FileUtils.byteCountToDisplaySize(usedMemory));
        valuesToWrite.add(DECIMAL_FORMAT.format(osSystemCpuLoad));
        valuesToWrite.add(DECIMAL_FORMAT.format(osProcessCpuLoad));
        valuesToWrite.add(DECIMAL_FORMAT.format(osProcessCpuTime));
        valuesToWrite.add(osSystemLoadAverage);
        valuesToWrite.add(cumulativeCount);
        valuesToWrite.add(BlockchainStrategy.getBlockchainFrameworkAsString());
        valuesToWrite.add(getStatisticType());
        valuesToWrite.add(GeneralConfiguration.HOST_ID);

        WriteStatistics.prepareWrite(GeneralConfiguration.LOG_PATH + GeneralConfiguration.SYSTEM_STATISTICS_FILE_NAME
                , HEADER, valuesToWrite);
    }

    @Override
    public StatisticType getStatisticType() {
        return StatisticType.General;
    }

    @SafeVarargs
    @Override
    public final <E> void printStatistics(final E... params) {
        LOG.info(toString());
    }

    @Override
    public String toString() {

        DECIMAL_FORMAT.setRoundingMode(RoundingMode.FLOOR);

        return "SystemStatistics{" +
                "osName='" + osName + '\'' +
                "; osArch='" + osArch + '\'' +
                "; osVersion='" + osVersion + '\'' +
                "; osNumberOfCores=" + osNumberOfCores +
                "; jvmAvailableProcessors=" + availableProcessors +
                "; osTotalMemory=" + FileUtils.byteCountToDisplaySize(osTotalMemory) +
                "; osFreePhysicalMemorySize=" + FileUtils.byteCountToDisplaySize(osFreePhysicalMemorySize) +
                "; osTotalSwapSpaceSize=" + FileUtils.byteCountToDisplaySize(osTotalSwapSpaceSize) +
                "; osFreeSwapSpaceSize=" + FileUtils.byteCountToDisplaySize(osFreeSwapSpaceSize) +
                "; osCommittedVirtualMemorySize=" + FileUtils.byteCountToDisplaySize(osCommittedVirtualMemorySize) +
                "; jvmTotalMemory=" + FileUtils.byteCountToDisplaySize(totalMemory) +
                "; jvmAvailableMemory=" + FileUtils.byteCountToDisplaySize(availableMemory) +
                "; jvmMaxMemory=" + FileUtils.byteCountToDisplaySize(maxMemory) +
                "; jvmFreeMemory=" + FileUtils.byteCountToDisplaySize(freeMemory) +
                "; jvmUsedMemory=" + FileUtils.byteCountToDisplaySize(usedMemory) +
                "; osSystemCpuLoad=" + DECIMAL_FORMAT.format(osSystemCpuLoad) +
                "; osProcessCpuLoad=" + DECIMAL_FORMAT.format(osProcessCpuLoad) +
                "; osProcessCpuTime=" + DECIMAL_FORMAT.format(osProcessCpuTime) +
                "; osSystemLoadAverage=" + osSystemLoadAverage +
                '}';
    }

    private void writeToDatabase(final int cumulativeCount) {

        Locale currentLocale = Locale.getDefault();
        DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols(currentLocale);
        decimalFormatSymbols.setDecimalSeparator('.');
        decimalFormatSymbols.setGroupingSeparator('.');
        DecimalFormat decimalFormat = (DecimalFormat) DECIMAL_FORMAT.clone();
        decimalFormat.setDecimalFormatSymbols(decimalFormatSymbols);

        try {

            String query = "insert into systemstatistics (os_name, os_arch, os_version, os_number_of_cores, " +
                    "jvm_available_processors, os_total_memory, os_free_physical_memory_size, " +
                    "os_total_swap_space_size, os_free_swap_space_size, os_committed_virtual_memory_size, " +
                    "jvm_total_memory, jvm_available_memory, jvm_max_memory, jvm_free_memory, jvm_used_memory, " +
                    "os_system_cpu_load, os_process_cpu_load, os_process_cpu_time, os_system_load_average, " +
                    "cumulative_count, basic_system, statistic_type, cid, run_id)" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement preparedStatement = PrepareStatementCollection.addPreparedStatement(query);
            preparedStatement.setString(1, osName);
            preparedStatement.setString(2, osArch);
            preparedStatement.setString(3, osVersion);
            preparedStatement.setInt(4, osNumberOfCores);
            preparedStatement.setInt(5, availableProcessors);
            preparedStatement.setString(6, FileUtils.byteCountToDisplaySize(osTotalMemory));
            preparedStatement.setString(7, FileUtils.byteCountToDisplaySize(osFreePhysicalMemorySize));
            preparedStatement.setString(8, FileUtils.byteCountToDisplaySize(osTotalSwapSpaceSize));
            preparedStatement.setString(9, FileUtils.byteCountToDisplaySize(osFreeSwapSpaceSize));
            preparedStatement.setString(10, FileUtils.byteCountToDisplaySize(osCommittedVirtualMemorySize));
            preparedStatement.setString(11, FileUtils.byteCountToDisplaySize(totalMemory));
            preparedStatement.setString(12, FileUtils.byteCountToDisplaySize(availableMemory));
            preparedStatement.setString(13, FileUtils.byteCountToDisplaySize(maxMemory));
            preparedStatement.setString(14, FileUtils.byteCountToDisplaySize(freeMemory));
            preparedStatement.setString(15, FileUtils.byteCountToDisplaySize(usedMemory));
            preparedStatement.setDouble(16, Double.parseDouble(decimalFormat.format(osSystemCpuLoad)));
            preparedStatement.setDouble(17, Double.parseDouble(decimalFormat.format(osProcessCpuLoad)));
            preparedStatement.setDouble(18, Double.parseDouble(decimalFormat.format(osProcessCpuTime)));
            preparedStatement.setDouble(19, osSystemLoadAverage);
            preparedStatement.setInt(20, cumulativeCount);
            preparedStatement.setString(21, BlockchainStrategy.getBlockchainFrameworkAsString());
            preparedStatement.setString(22, getStatisticType().name());
            preparedStatement.setString(23, GeneralConfiguration.HOST_ID);
            preparedStatement.setString(24, GeneralConfiguration.RUN_ID);

            //preparedStatement.executeUpdate();
            preparedStatement.addBatch();

        } catch (SQLException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
