package corda.supplements;

import co.paralleluniverse.fibers.Suspendable;
import net.corda.core.identity.Party;
import net.corda.core.identity.PartyAndCertificate;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.node.NodeInfo;
import org.apache.log4j.Logger;

public final class CordaDebugHelper {

    private static final Logger LOG = Logger.getLogger(CordaDebugHelper.class);

    private CordaDebugHelper() {
    }

    @Suspendable
    public static void printDebug(final CordaRPCOps proxy) {
        for (final NodeInfo nodeInfo : proxy.networkMapSnapshot()) {
            for (final Party legalIdentity : nodeInfo.getLegalIdentities()) {
                LOG.info("Legal identity: " + legalIdentity.toString());
            }
        }

        for (final Party notaryIdentity : proxy.notaryIdentities()) {
            LOG.info("Notary detected: " + notaryIdentity.getName());
        }

        for (final NodeInfo nodeInfo : proxy.networkMapSnapshot()) {
            LOG.info("Party detected: " + nodeInfo.toString());
        }

        for (final NodeInfo nodeInfo : proxy.networkMapSnapshot()) {
            for (final PartyAndCertificate pac : nodeInfo.getLegalIdentitiesAndCerts()) {
                LOG.info("Party detected: " + pac.getParty());
            }
        }

        for (final Party legalIdentity : proxy.nodeInfo().getLegalIdentities()) {
            LOG.info("Legal identity: " + legalIdentity.getName());
        }

        for (final String flow : proxy.registeredFlows()) {
            LOG.info("Flow detected: " + flow);
        }

    }

}
