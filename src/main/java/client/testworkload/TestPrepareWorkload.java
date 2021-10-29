package client.testworkload;

import client.client.ClientObject;
import client.commoninterfaces.IPrepareWorkload;
import client.statistics.IStatistics;
import org.apache.log4j.Logger;

import java.util.Queue;

public class TestPrepareWorkload implements IPrepareWorkload {

    private static final Logger LOG = Logger.getLogger(TestPrepareWorkload.class);

    @Override
    public <E> E prepareWorkload(final E... params) {
        LOG.info("Establishing connection for client... " + ((ClientObject) params[0]).getClientId());
        return null;
    }

    @Override
    public <E> E endPrepareWorkload(final E... params) {
        LOG.info("Ending workload for client... Step 2 " + ((ClientObject) params[0]).getClientId());
        return null;
    }

    @Override
    public synchronized <E> Queue<IStatistics> getStatistics(final E... params) {
        return null;
    }
}
