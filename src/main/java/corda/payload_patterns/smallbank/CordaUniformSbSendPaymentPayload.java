package corda.payload_patterns.smallbank;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import com.smallbankflow.flows.SmallBankFlow;
import com.smallbankflow.states.SmallBankState;
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

public class CordaUniformSbSendPaymentPayload implements ICordaPayloads {

    private static final Logger LOG = Logger.getLogger(CordaUniformSbSendPaymentPayload.class);

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
                Vault.StateStatus.UNCONSUMED, Collections.singleton(SmallBankState.class), null, null, null,
                null, Vault.RelevancyStatus.ALL, Collections.EMPTY_SET, Collections.EMPTY_SET,
                null, Collections.EMPTY_LIST, null);
        PageSpecification pageSpecification = new PageSpecification(Configuration.DEFAULT_PAGE_NUMBER,
                Configuration.DEFAULT_PAGE_SIZE);
        //Sort sort = new Sort(Collections.emptyList());
        Sort.Attribute attribute = Sort.VaultStateAttribute.RECORDED_TIME;
        SortAttribute sortAttribute = new SortAttribute.Standard(attribute);
        Sort.SortColumn sortColumn = new Sort.SortColumn(sortAttribute, Sort.Direction.DESC);
        Sort sort = new Sort(Collections.singleton(sortColumn));

        int id = REQUEST_COUNTER.updateAndGet(value -> (value % Configuration.SEND_CYCLE == 0) ? 1 : value + 1);

        if(Configuration.SEND_CYCLE > (GeneralConfiguration.CLIENT_COUNT * GeneralConfiguration.CLIENT_WORKLOADS.get(0)
                * Configuration.NUMBER_OF_TRANSACTIONS_PER_CLIENT)) {
            LOG.error("More cycles than accounts available, aborting");
            System.exit(1);
        }

        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        // global start id (string) - offset (host) (string) - local id (int)
        String prefix_sender; //= GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;
        String prefix_destination; //= GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;
        if(id % Configuration.SEND_CYCLE == 0) {
            prefix_sender = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;
            prefix_destination = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + 1;
            LOG.debug("Reset id destination: " + prefix_destination + " sender: " + prefix_sender);
        } else {
            prefix_sender = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + id;
            prefix_destination = GeneralConfiguration.RUN_ID.split("-")[0] + "-" + GeneralConfiguration.HOST_ID + "-" + (id + 1);
        }

        List<Object> values = new ArrayList<>();

        values.add(SmallBankFlow.class);
        values.add("SendPayment");

        List<String> params = new ArrayList<>();
        params.add(prefix_sender);
        params.add(prefix_destination);
        params.add(String.valueOf(Configuration.SEND_PAYMENT_AMOUNT));
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
        iCordaWritePayload.setEventPrefix("sendPayment ");
        iCordaWritePayload.setSpecificPayloadType("sendPayment");

        return ImmutablePair.of(values, signature);
    }

}
