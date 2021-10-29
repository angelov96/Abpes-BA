package corda.connection;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import corda.configuration.Configuration;
import corda.configuration.CordaClientConfiguration;
import net.corda.client.rpc.CordaRPCClient;
import net.corda.client.rpc.CordaRPCClientConfiguration;
import net.corda.client.rpc.GracefulReconnect;
import net.corda.client.rpc.RPCException;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.utilities.NetworkHostAndPort;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class Rpc {

    private static final Logger LOG = Logger.getLogger(Rpc.class);

    @Suspendable
    public CordaRPCClient prepareRpcConnection(final List<String> nodes) {

        List<NetworkHostAndPort> nodeList = new ArrayList<>();
        nodes.forEach(node -> nodeList.add(NetworkHostAndPort.parse(node)));

        CordaRPCClientConfiguration cordaRPCClientConfiguration = Configuration.USE_CUSTOM_CLIENT_CONFIGURATION ?
                new CordaClientConfiguration().getCustomClientConfiguration() : CordaRPCClientConfiguration.DEFAULT;

        return new CordaRPCClient(nodeList, cordaRPCClientConfiguration);
    }

    @Suspendable
    public CordaRPCOps startRpcClient(final String rpcUsername, final String rpcPassword,
                                      final CordaRPCClient client) {
        boolean hasError;
        int i = 0;
        CordaRPCOps proxy = null;
        do {
            try {
                proxy = client.start(rpcUsername, rpcPassword, gracefulReconnect()).getProxy();
                hasError = false;
            } catch (RPCException ex) {
                ExceptionHandler.logException(ex);
                hasError = true;
            }
            if (hasError) {
                try {
                    Strand.sleep(Configuration.RECONNECTION_SLEEP_TIME);
                } catch (SuspendExecution | InterruptedException exception) {
                    ExceptionHandler.logException(exception);
                } finally {
                    i++;
                }
            }
        } while (hasError && i < Configuration.MAX_CONNECTION_RETRIES);
        return proxy;
    }

    @Suspendable
    private GracefulReconnect gracefulReconnect() {
        return new GracefulReconnect(onReconnect(), onDisconnect(), Configuration.MAX_GRACEFUL_RECONNECTS);
    }

    @Suspendable
    Runnable onReconnect() {
        return new Runnable() {
            @Override
            public void run() {
                LOG.info("Reconnecting");
            }
        };
    }

    @Suspendable
    Runnable onDisconnect() {
        return new Runnable() {
            @Override
            public void run() {
                LOG.info("Disconnected");
            }
        };
    }

}
