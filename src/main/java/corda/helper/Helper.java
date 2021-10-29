package corda.helper;

import co.paralleluniverse.fibers.Suspendable;
import net.corda.core.crypto.SecureHash;
import net.corda.core.identity.Party;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.node.NodeInfo;
import net.corda.core.node.services.vault.PageSpecification;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public final class Helper {

    private static final Logger LOG = Logger.getLogger(Helper.class);

    private Helper() {
    }

    @Suspendable
    @SafeVarargs
    public static <E> Object[] unpack(final E... objects) {
        List<Object> list = new ArrayList<>();
        for (final Object object : objects) {
            if (object instanceof Object[]) {
                list.addAll(Arrays.asList((Object[]) object));
            } else if (object instanceof List<?>) {
                list.addAll((Collection<?>) object);
            } else {
                list.add(object);
            }
        }

        return list.toArray(new Object[0]);
    }

    @Suspendable
    public static void addTransactionNote(final CordaRPCOps proxy, final SecureHash txId,
                                          final String transactionNote) {
        proxy.addVaultTransactionNote(txId, transactionNote);
    }

    @Suspendable
    public static List<Party> getPartiesWithoutNotaries(final CordaRPCOps proxy) {
        return getParties(proxy, true);
    }

    @Suspendable
    public static List<Party> getParties(final CordaRPCOps proxy, final boolean excludeNotaries) {
        List<NodeInfo> nodeInfos = proxy.networkMapSnapshot();
        List<net.corda.core.identity.Party> partyList = new ArrayList<>();

        nodeInfos.forEach(nodeInfo -> nodeInfo.getLegalIdentities().forEach(legalIdentity -> {
            if ((!proxy.notaryIdentities().contains(legalIdentity) && !partyList.contains(legalIdentity))
                    || (!excludeNotaries  && !partyList.contains(legalIdentity))) {
                LOG.debug("Added: " + legalIdentity + " exclude notaries: " + excludeNotaries);
                partyList.add(legalIdentity);
            } else {
                LOG.warn("Excluded: " + legalIdentity);
            }
        }));

        return partyList;
    }

    @Suspendable
    public static List<String> getNotariesAsStringList(final CordaRPCOps proxy) {
        List<Party> notaries = getOnlyNotaries(proxy);
        List<String> notariesAsString = new ArrayList<>();
        notaries.forEach(notary -> notariesAsString.add(notary.getName().toString()));

        return notariesAsString;
    }

    @Suspendable
    public static List<Party> getOnlyNotaries(final CordaRPCOps proxy) {
        List<NodeInfo> nodeInfos = proxy.networkMapSnapshot();
        List<net.corda.core.identity.Party> partyList = new ArrayList<>();
        nodeInfos.forEach(nodeInfo -> nodeInfo.getLegalIdentities().forEach(subNodeInfo -> proxy.notaryIdentities().forEach(notary -> {
            if ((notary.getOwningKey().equals(subNodeInfo.getOwningKey()) && !partyList.contains(subNodeInfo))) {
                LOG.debug("Added notary: " + subNodeInfo);
                partyList.add(subNodeInfo);
            }
        })));
        return partyList;
    }

    @Suspendable
    public static boolean isDefaultPageSpecification(int pageNumber, int pageSize) {
        return new PageSpecification(pageNumber, pageSize).isDefault();
    }

}
