package corda.read;

import co.paralleluniverse.fibers.Suspendable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import corda.configuration.Configuration;
import corda.payloads.CustomRecursiveToStringStyle;
import corda.payloads.ICordaReadPayload;
import corda.statistics.ReadStatisticObject;
import net.corda.core.contracts.ContractState;
import net.corda.core.contracts.StateAndRef;
import net.corda.core.contracts.StateRef;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.node.services.Vault;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class Read implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(Read.class);
    private static final int START = 0;
    private static final int MAX_LEVEL = 10;

    private final Multimap<String, Map.Entry<String, Instant>> stateMap = HashMultimap.create();

    @Suspendable
    public void extendedRead(final CordaRPCOps proxy, final QueryCriteria queryCriteria,
                             final Class<? extends ContractState> clazz, final PageSpecification pageSpecification,
                             final Sort sort) {
        Vault.Page<? extends ContractState> dataFeed = proxy.vaultQueryBy(queryCriteria, pageSpecification,
                sort,
                clazz);

        LOG.trace(dataFeed.toString());

        LOG.debug("Size of queried data: " + dataFeed.getStates().size());
        LOG.debug("Total states available: " + ((dataFeed.getTotalStatesAvailable() == -1) ? "none" :
                dataFeed.getTotalStatesAvailable()));
        getStateInfos(dataFeed);

        getOtherResults(dataFeed);

        getStateRef(proxy, dataFeed, clazz);

        getStateMetadata(dataFeed);

    }

    @Suspendable
    public void getStateRef(final CordaRPCOps proxy, final Vault.Page<? extends ContractState> dataFeed, final Class<
            ? extends ContractState> clazz) {
        for (final StateAndRef<? extends ContractState> stateStateAndRef : dataFeed.getStates()) {

            LOG.debug("Total number of states: " + ((dataFeed.getTotalStatesAvailable() == -1) ? "none" :
                    dataFeed.getTotalStatesAvailable()));

            LOG.debug("Referenced transaction hash: " + stateStateAndRef.referenced().getStateAndRef().getRef().getTxhash());
            StateRef stateRef = new StateRef(stateStateAndRef.getRef().getTxhash(),
                    stateStateAndRef.getRef().getIndex());
            LOG.debug("Hash of stateref: " + stateRef.getTxhash().toString());

            Map<StateRef, List<StateRef>> stateRefListMap = processStates(new LinkedHashMap<>(),
                    proxy, null, new PageSpecification(),
                    new Sort(Collections.emptyList()),
                    stateRef, START, MAX_LEVEL, clazz);

            for (final Map.Entry<StateRef, List<StateRef>> stateRefListEntry : stateRefListMap.entrySet()) {
                List<StateRef> values = stateRefListEntry.getValue();
                for (final StateRef ref : values) {
                    LOG.debug("Process contains txHash: " + ref.getTxhash().toString());
                }

            }

            LOG.debug("StateRef contract: " + stateStateAndRef.getState().getContract());
            LOG.debug("StateRef notary: " + stateStateAndRef.getState().getNotary());
            LOG.debug("StateRef encumbrance: " + stateStateAndRef.getState().getEncumbrance());
            LOG.debug("StateRef constraint: " + stateStateAndRef.getState().getConstraint());

            LOG.trace("Datafeed: " + dataFeed.toString());
            handleConnectedStates();

            LOG.trace("Datafeed states: " + dataFeed.getStates().toString());

            LOG.debug("StateRef ref: " + stateStateAndRef.getRef().toString());
        }

        LOG.debug("State types: " + dataFeed.getStateTypes());
    }

    @Suspendable
    private void handleConnectedStates() {

        for (final Map.Entry<String, Collection<Map.Entry<String, Instant>>> entry : stateMap.asMap().entrySet()) {
            LOG.info("Key value: " + entry.getKey() + " values mapped to: "
                    + entry.getValue());
            HashMap<String, Long> tempMap = new LinkedHashMap<>();
            for (final Map.Entry<String, Instant> stringInstantEntry : entry.getValue()) {
                tempMap.put(stringInstantEntry.getKey(),
                        (stringInstantEntry.getValue() == null ?
                                Long.MAX_VALUE :
                                stringInstantEntry.getValue().getEpochSecond()
                        ));
            }

            LinkedHashMap<String, Long> sorted = tempMap
                    .entrySet()
                    .stream()
                    .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                    .collect(
                            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                    LinkedHashMap::new));

            LOG.debug(sorted);

        }
    }

    @Suspendable
    public Map<StateRef, List<StateRef>> processStates(final Map<StateRef, List<StateRef>> stateMap,
                                                       final CordaRPCOps proxy,
                                                       final QueryCriteria queryCriteria,
                                                       final PageSpecification pageSpecification,
                                                       final Sort sort,
                                                       final StateRef stateRef, final int start, final int maxLevel,
                                                       final Class<? extends ContractState> clazz) {
        LOG.debug(("StateRefHash (processStates): " + stateRef.getTxhash()));
        QueryCriteria generalCriteria = null;
        if (queryCriteria == null) {
            generalCriteria = new QueryCriteria.VaultQueryCriteria(Vault.StateStatus.ALL,
                    null, Collections.singletonList(stateRef), null, null, null,
                    Vault.RelevancyStatus.ALL, Collections.EMPTY_SET, Collections.EMPTY_SET, null, Collections.EMPTY_LIST
                    , null);
        }
        Vault.Page<?> datafeed = proxy.vaultQueryBy((queryCriteria == null ? generalCriteria :
                queryCriteria), pageSpecification, sort, clazz);

        if (datafeed.getStates().size() == 0 || start > maxLevel) {
            return stateMap;
        }

        for (int i = 0; datafeed.getStates().size() != 0; i++) {
            StateRef ref = datafeed.getStates().get(i).referenced().getStateAndRef().getRef();

            LOG.debug("StateRefHash retrieved from data feed: " + ref.getTxhash().toString());
            LOG.debug("Size retrieved from data feed: " + datafeed.getStates().size());

            if (!stateMap.containsKey(stateRef)) {
                List<StateRef> stateRefList = new ArrayList<>();
                stateRefList.add(ref);
                stateMap.put(stateRef, stateRefList);
            } else {
                List<StateRef> existingStateRefList = stateMap.get(stateRef);
                existingStateRefList.add(ref);
                stateMap.put(stateRef, existingStateRefList);
            }

            LOG.debug("New referenced transaction hash: " + datafeed.getStates().get(i).referenced().getStateAndRef().getRef().getTxhash());
            LOG.debug("Added transaction hash: " + ref.getTxhash().toString());

            if (stateRef.getTxhash().equals(ref.getTxhash())) {
                return stateMap;
            }

            processStates(stateMap, proxy, queryCriteria, pageSpecification, sort, ref, start + 1, maxLevel, clazz);
        }
        return stateMap;
    }

    @Suspendable
    private void getStateMetadata(final Vault.Page<? extends ContractState> dataFeed) {
        for (final Vault.StateMetadata statesMetadatum : dataFeed.getStatesMetadata()) {
            LOG.debug("State metadatum: " + statesMetadatum.toString());

            LOG.debug("Constraint type: " + Objects.requireNonNull(statesMetadatum.getConstraintInfo()).type());
            LOG.debug("Constraint info: " + Objects.requireNonNull(statesMetadatum.getConstraintInfo()).getConstraint());
            LOG.debug(("Constraint: " + statesMetadatum.getConstraintInfo()));
            LOG.debug("Consumed time: " + statesMetadatum.getConsumedTime());
            LOG.debug("Contract state name: " + statesMetadatum.getContractStateClassName());
            LOG.debug("Lock id: " + statesMetadatum.getLockId());
            LOG.debug("Lock update time: " + statesMetadatum.getLockUpdateTime());
            LOG.debug("Notary: " + statesMetadatum.getNotary());
            LOG.debug("Recorded time: " + statesMetadatum.getRecordedTime());
            // outpoint in BTC
            LOG.debug("Ref: " + statesMetadatum.getRef());
            LOG.debug("Relevancy Status: " + statesMetadatum.getRelevancyStatus());
            LOG.debug("Status: " + statesMetadatum.getStatus());
        }
    }

    /*
      @NotNull
      private final StateStatus status;
      @Nullable
      private final Set contractStateTypes;
      @Nullable
      private final List stateRefs;
      @Nullable
      private final List notary;
      @Nullable
      private final QueryCriteria.SoftLockingCondition softLockingCondition;
      @Nullable
      private final QueryCriteria.TimeCondition timeCondition;
      @NotNull
      private final RelevancyStatus relevancyStatus;
      @NotNull
      private final Set constraintTypes;
      @NotNull
      private final Set constraints;
      @Nullable
      private final List participants;
      @NotNull
      private final List externalIds;
      @Nullable
      private final List exactParticipants;
      */

    @Suspendable
    private void getOtherResults(final Vault.Page<? extends ContractState> dataFeed) {
        LOG.debug("Other results size: " + dataFeed.getOtherResults().size());
        for (final Object o : dataFeed.getOtherResults()) {
            LOG.debug("Other result: " + o.toString());
        }
    }

    @Suspendable
    private void getStateInfos(final Vault.Page<? extends ContractState> dataFeed) {
        for (int i = 0; i < dataFeed.getStates().size(); i++) {
            LOG.info("TxHash: " + dataFeed.getStates().get(i).getRef().getTxhash());
            LOG.info("Referenced status: " + dataFeed.getStates().get(i).referenced().getStateAndRef().getRef().getTxhash());
            LOG.info("Reference: " + dataFeed.getStatesMetadata().get(i).getRef());
        }
    }

    @Suspendable
    public Vault.Page<? extends ContractState> simpleRead(final CordaRPCOps proxy,
                                                          final Class<? extends ContractState> clazz) {
        return proxy.vaultQuery(clazz);
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> read(final E... params) {
        if (params.length == 3) {

            CordaRPCOps proxy = (CordaRPCOps) params[0];
            ICordaReadPayload iCordaReadPayload = (ICordaReadPayload) params[1];
            ReadStatisticObject readStatisticObject = (ReadStatisticObject) params[2];

            return read(proxy, iCordaReadPayload, readStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> read(final CordaRPCOps proxy,
                                                final ICordaReadPayload iCordaReadPayload,
                                                final ReadStatisticObject readStatisticObject) {

        readStatisticObject.setStartTime(System.nanoTime());

        Vault.Page<? extends ContractState> dataFeed = proxy.vaultQueryBy(iCordaReadPayload.getVaultQueryCriteria(),
                iCordaReadPayload.getPageSpecification(), iCordaReadPayload.getSort(), iCordaReadPayload.getClazz());
        if (Configuration.RECEIVE_READ_REQUEST) {
            LOG.debug("Size of queried data: " + dataFeed.getStates().size());
            LOG.debug("Total states available: " + ((dataFeed.getTotalStatesAvailable() == -1) ? "none" :
                    dataFeed.getTotalStatesAvailable()));

            if (dataFeed.getStates().size() == 0) {
                LOG.error("Size of decoded types equal 0, this might be an error");
            }

            dataFeed.getStates().forEach(payload -> LOG.debug(
                    ReflectionToStringBuilder.toString(iCordaReadPayload.getClazz().cast(payload.getState().getData()),
                            new CustomRecursiveToStringStyle(Configuration.STRING_STYLE_MAX_RECURSIVE_DEPTH))));

            readStatisticObject.setEndTime(System.nanoTime());
            return ImmutablePair.of(false, "");
        } else {
            readStatisticObject.setEndTime(-1);
            return ImmutablePair.of(false, "");
        }
    }

}
