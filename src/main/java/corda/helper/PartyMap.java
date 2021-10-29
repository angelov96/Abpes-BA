package corda.helper;

import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import net.corda.core.identity.Party;
import net.corda.core.messaging.CordaRPCOps;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class PartyMap {

    private static final Logger LOG = Logger.getLogger(PartyMap.class);

    private static final Map<String, List<String>> NOTARIES_AS_STRINGS = new ConcurrentHashMap<>();
    private static final Map<String, List<Party>> PARTIES_WITHOUT_NOTARIES = new ConcurrentHashMap<>();
    private static final Map<String, List<Party>> ONLY_NOTARIES = new ConcurrentHashMap<>();
    private static final Map<String, List<Party>> PARTIES = new ConcurrentHashMap<>();

    @Suspendable
    public static Map<String, List<String>> getNotariesAsStrings() {
        Map<String, List<String>> stringListMap = deepCopy(NOTARIES_AS_STRINGS);
        stringListMap.forEach((s, l) -> Collections.sort(l));
        return stringListMap;
    }

    @Suspendable
    public static Map<String, List<Party>> getPartiesWithoutNotaries() {
        Map<String, List<Party>> stringListMap = deepCopy(PARTIES_WITHOUT_NOTARIES);
        stringListMap.forEach((s, l) ->
                l.stream()
                        .sorted(Comparator.comparing(party -> party.getName().toString())));
        return stringListMap;
    }

    @Suspendable
    public static List<Party> getRandomSigningParty(final String clientId, final CordaRPCOps proxy) {
        List<Party> parties = getPartiesWithoutNotaries().get(clientId);

        for (final Party proxyNode : proxy.nodeInfo().getLegalIdentities()) {
            parties.remove(proxyNode);
            LOG.info("Removed party (reflexive) " + proxyNode.toString());
        }

        return GenericSelectionStrategy.selectRandom(parties, 1, true);
    }

    @Suspendable
    public static List<Party> getAllSigningParties(final String clientId, final CordaRPCOps proxy) {
        List<Party> parties = getPartiesWithoutNotaries().get(clientId);

        for (final Party proxyNode : proxy.nodeInfo().getLegalIdentities()) {
            parties.remove(proxyNode);
            LOG.info("Removed party (reflexive) " + proxyNode.toString());
        }

        return parties;
    }

    @Suspendable
    public static Map<String, List<Party>> getOnlyNotaries() {
        Map<String, List<Party>> stringListMap = deepCopy(ONLY_NOTARIES);
        stringListMap.forEach((s, l) ->
                l.stream()
                        .sorted(Comparator.comparing(party -> party.getName().toString())));
        return stringListMap;
    }

    @Suspendable
    public static Map<String, List<Party>> getParties() {
        Map<String, List<Party>> stringListMap = deepCopy(PARTIES);
        stringListMap.forEach((s, l) ->
                l.stream()
                        .sorted(Comparator.comparing(party -> party.getName().toString())));
        return stringListMap;
    }

    @Suspendable
    public static <T> Map<String, List<T>> deepCopy(final Map<String, List<T>> original) {
        return original
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, valueMapper -> new ArrayList<>(valueMapper.getValue())));
    }

    @Suspendable
    public static Map<String, List<String>> getNotariesAsStringsUnsorted() {
        return NOTARIES_AS_STRINGS;
    }

    @Suspendable
    public static Map<String, List<Party>> getPartiesWithoutNotariesUnsorted() {
        return PARTIES_WITHOUT_NOTARIES;
    }

    @Suspendable
    public static Map<String, List<Party>> getOnlyNotariesUnsorted() {
        return ONLY_NOTARIES;
    }

    @Suspendable
    public static Map<String, List<Party>> getPartiesUnsorted() {
        return PARTIES;
    }

}
