package diem.workloads;

import client.client.ClientObject;
import client.client.ClientRegistry;
import client.commoninterfaces.IPrepareWorkload;
import client.commoninterfaces.IRequestDistribution;
import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import com.diem.AuthKey;
import com.diem.DiemClient;
import com.diem.Ed25519PrivateKey;
import com.diem.PrivateKey;
import com.diem.types.ChainId;
import corda.listener.Listen;
import diem.configuration.Configuration;
import diem.connection.JSONRpc;
import org.apache.log4j.Logger;
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PrepareDiemWorkload implements IPrepareWorkload, IRequestDistribution {

    private static final Logger LOG = Logger.getLogger(PrepareDiemWorkload.class);
    private static final Queue<String> PREPARED_ACCOUNT_LIST = new ConcurrentLinkedQueue<>();
    private final List<Object> paramList = new ArrayList<>();
    private final Queue<IStatistics> iStatistics = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger LISTENER_COUNTER = new AtomicInteger(0);

    @Suspendable
    public <E> List<E> getParams() {
        return (List<E>) paramList;
    }

    @Override
    public <E> E prepareWorkload(E... params) {
        ClientObject clientObject = (ClientObject) params[0];

        ChainId CHAIN_ID = new ChainId((byte) Configuration.CHAIN_ID);
        DiemClient diemClient = new JSONRpc().createDiemClient(Configuration.JSON_RPC_URL, CHAIN_ID);

        PrepareDiemWorkloadObject prepareDiemWorkloadObject = new PrepareDiemWorkloadObject();

        PrivateKey senderPrivateKey = new Ed25519PrivateKey(new Ed25519PrivateKeyParameters(new SecureRandom()));
        AuthKey senderAuthKey = AuthKey.ed25519(senderPrivateKey.publicKey());

        return null;
    }

    @Suspendable
    private void prepareListener(final ClientObject clientObject,
                                 final PrepareDiemWorkloadObject prepareDiemWorkloadObject) {
        if (diem.configuration.Configuration.ENABLE_LISTENER) {
            if (LISTENER_COUNTER.getAndIncrement() < diem.configuration.Configuration.NUMBER_OF_LISTENERS) {
                LOG.info("Registering listener for: " + clientObject.getClientId());

                Listen listen =
                        new Listen(GeneralConfiguration.CLIENT_COUNT *
                                GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                Collections.singletonList(0), false).get(0) *
                                diem.configuration.Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT,
                                GeneralConfiguration.CLIENT_COUNT *
                                        GenericSelectionStrategy.selectFixed(GeneralConfiguration.CLIENT_WORKLOADS,
                                        Collections.singletonList(0),
                                        false).get(0) *
                                        diem.configuration.Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT,
                                diem.configuration.Configuration.LISTENER_THRESHOLD,
                                diem.configuration.Configuration.LISTENER_TOTAL_THRESHOLD,
                                iStatistics,
                                ClientRegistry.getClientObjects());


            }
        }

    }

    @Suspendable
    private void prepareWritePayloads(final DiemClient diemClient,
                                      final PrepareDiemWorkloadObject prepareDiemWorkloadObject,
                                      final ClientObject clientObject) {
        if (diem.configuration.Configuration.PREPARE_WRITE_PAYLOADS) {

        }

    }

    @Suspendable
    private void prepareReadPayloads(final PrepareDiemWorkloadObject prepareDiemWorkloadObject,
                                     final ClientObject clientObject) {
        if (diem.configuration.Configuration.PREPARE_READ_PAYLOADS) {

        }

    }

    @Override
    public <E> E endPrepareWorkload(final E... params) {
        LOG.info(((ClientObject) params[0]).getClientId() + " client preparation ended");
        return null;
    }

    @Override
    public <E> Queue<IStatistics> getStatistics(final E... params) {
        return iStatistics;
    }

    @Override
    public <E> void handleRequestDistribution(final E... params) {
    }
}
