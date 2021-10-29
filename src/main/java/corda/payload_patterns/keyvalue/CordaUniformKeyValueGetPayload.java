package corda.payload_patterns.keyvalue;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import com.keyvalueflow.flows.KeyValueFlow;
import com.keyvalueflow.states.KeyValueState;
import corda.configuration.Configuration;
import corda.helper.PartyMap;
import corda.payload_patterns.ICordaPayloads;
import corda.payloads.ICordaWritePayload;
import net.corda.core.identity.CordaX500Name;
import net.corda.core.identity.Party;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.node.services.Vault;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import net.corda.core.node.services.vault.SortAttribute;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class CordaUniformKeyValueGetPayload implements ICordaPayloads {

    private static final Logger LOG = Logger.getLogger(CordaUniformKeyValueGetPayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<ICordaWritePayload> getPayloads(final E... params) {

        if (params.length != 3) {
            throw new IllegalArgumentException("Expecting exactly 3 arguments for: " + this.getClass().getName());
        }

        return preparePayloads(params);
    }

    @Suspendable
    @SafeVarargs
    private final <E> List<ICordaWritePayload> preparePayloads(final E... values) {
        List<ICordaWritePayload> payLoadList = new ArrayList<>();

        for (int i = 0; i < (Integer) values[2]; i++) {
            ICordaWritePayload iCordaWritePayload = null;
            try {
                iCordaWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
                ExceptionHandler.logException(ex);
            }

            ImmutablePair<List<Object>, String> payload = createPayload((CordaRPCOps) values[0],
                    (ClientObject) values[1], i, Objects.requireNonNull(iCordaWritePayload));
            List<Object> objectList = payload.getLeft();

            Objects.requireNonNull(iCordaWritePayload).setValues(objectList);
            Objects.requireNonNull(iCordaWritePayload).setSignature(payload.getRight());

            payLoadList.add(iCordaWritePayload);
        }

        return payLoadList;
    }

    private static final AtomicInteger REQUEST_COUNTER = new AtomicInteger(0);

    @Suspendable
    private ImmutablePair<List<Object>, String> createPayload(final CordaRPCOps proxy,
                                                              final ClientObject clientObject, final int i,
                                                              final ICordaWritePayload iCordaWritePayload) {
        //List<String> notariesAsString = Helper.getNotariesAsStringList(proxy);

        CordaX500Name parse = CordaX500Name.parse(
                GenericSelectionStrategy.selectFixed(PartyMap.getNotariesAsStrings().get(clientObject.getClientId()), Collections.singletonList(Integer.parseInt(GeneralConfiguration.HOST_ID.split("-")[5])), true).get(0)
                //GenericSelectionStrategy.selectFixed(PartyMap.getNotariesAsStrings().get(clientObject.getClientId()), Collections.singletonList(0), true).get(0)
                //GenericSelectionStrategy.selectRoundRobin(PartyMap.getNotariesAsStrings().get(clientObject.getClientId()), 1, true, false, "rr-wl-notary", 1, false).get(0)
                /*GenericSelectionStrategy.selectRoundRobin(notariesAsString, 1, true, false, "rr-wl-notary", 1,
                false).get(0)*/);
        Party notary = proxy.notaryPartyFromX500Name(parse);

        //List<Party> partyList = Helper.getPartiesWithoutNotaries(proxy);
        //List<Party> partyList = PartyMap.getPartiesWithoutNotaries().get(clientObject.getClientId());

        QueryCriteria.VaultQueryCriteria vaultQueryCriteria = new QueryCriteria.VaultQueryCriteria(
                Vault.StateStatus.UNCONSUMED, Collections.singleton(KeyValueState.class), null, null, null,
                null, Vault.RelevancyStatus.ALL, Collections.EMPTY_SET, Collections.EMPTY_SET,
                null, Collections.EMPTY_LIST, null);
        PageSpecification pageSpecification = new PageSpecification(Configuration.DEFAULT_PAGE_NUMBER,
                Configuration.DEFAULT_PAGE_SIZE);
        //Sort sort = new Sort(Collections.emptyList());
        Sort.Attribute attribute = Sort.VaultStateAttribute.RECORDED_TIME;
        SortAttribute sortAttribute = new SortAttribute.Standard(attribute);
        Sort.SortColumn sortColumn = new Sort.SortColumn(sortAttribute, Sort.Direction.DESC);
        Sort sort = new Sort(Collections.singleton(sortColumn));

        int id = REQUEST_COUNTER.incrementAndGet();
        String signature = System.currentTimeMillis() + RandomStringUtils.random(10, true, true);
        String key = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;

        List<Object> values = new ArrayList<>();
        values.add(KeyValueFlow.class);
        values.add("Get");
        List<String> params = new ArrayList<>();
        params.add(key);
        params.add("true");
        params.add(signature);
        values.add(params);
        values.add(notary);

        /*for (final Party proxyNode : proxy.nodeInfo().getLegalIdentities()) {
            partyList.remove(proxyNode);
            LOG.info("Removed party (reflexive) " + proxyNode.toString());
        }*/

        //List<Party> partyList = PartyMap.getRandomSigningParty(clientObject.getClientId(), proxy);
        List<Party> partyList = PartyMap.getAllSigningParties(clientObject.getClientId(), proxy);

        values.add(partyList);
        //values.add(partyList.subList(2, 3));
        values.add(vaultQueryCriteria);
        values.add(pageSpecification);
        values.add(sort);

        iCordaWritePayload.setValueToRead(null);
        iCordaWritePayload.setEventPrefix("keyValue/get ");
        iCordaWritePayload.setSpecificPayloadType("keyValue/get");

        return ImmutablePair.of(values, signature);
    }

}
