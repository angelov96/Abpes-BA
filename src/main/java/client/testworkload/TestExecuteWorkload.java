package client.testworkload;

import client.client.ClientObject;
import client.commoninterfaces.IExecuteWorkload;
import client.executors.ClientExecutorPool;
import client.statistics.GeneralStatisticObject;
import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class TestExecuteWorkload implements IExecuteWorkload {

    private static final Logger LOG = Logger.getLogger(ClientExecutorPool.class);

    private final AtomicInteger numberOfRequest = new AtomicInteger(0);

    private final Queue<GeneralStatisticObject> generalStatisticObjects =
            new ConcurrentLinkedQueue<>();

    @Suspendable
    @Override
    public <E> E executeWorkload(final E... params) {
        for (int i = 0; i < 10; i++) {
            GeneralStatisticObject generalStatisticObject = new GeneralStatisticObject();
            generalStatisticObject.setStartTime(System.nanoTime(), false);
            LOG.info("Executing workload by client: " + ((ClientObject) params[1]).getClientId());
            generalStatisticObject.setEndTime(System.nanoTime(), false);
            generalStatisticObjects.add(generalStatisticObject);
        }
        return null;
    }

    @Suspendable
    @Override
    public <E> E endWorkload(final E... params) {
        LOG.info("Ended workload Step 1 for: " + ((ClientObject) params[1]).getClientId());
        return null;
    }

    @Override
    public synchronized <E> Queue<IStatistics> getStatistics(final E... params) {
        return null;
    }
}
