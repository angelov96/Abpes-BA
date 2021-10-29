package corda.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import com.keyvalueflow.flows.KeyValueFlow;
import corda.configuration.Configuration;
import corda.helper.Helper;
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

public class CordaSinglePayload implements ICordaPayloads {

    private static final Logger LOG = Logger.getLogger(CordaSinglePayload.class);

    @SafeVarargs
    @Override
    @Suspendable
    public final <E> List<ICordaWritePayload> getPayloads(final E... params) {

        if (params.length != 2) {
            throw new IllegalArgumentException("Expecting exactly 2 arguments for: " + this.getClass().getName());
        }

        return preparePayloads(params);
    }

    @Suspendable
    @SafeVarargs
    private final <E> List<ICordaWritePayload> preparePayloads(final E... values) {
        List<ICordaWritePayload> payLoadList = new ArrayList<>();

        ICordaWritePayload iCordaWritePayload = null;
        try {
            iCordaWritePayload = Configuration.WRITE_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        ImmutablePair<List<Object>, String> payload = createPayload((CordaRPCOps) values[0],
                (ClientObject) values[1], Objects.requireNonNull(iCordaWritePayload));
        List<Object> objectList = payload.getLeft();

        Objects.requireNonNull(iCordaWritePayload).setValues(objectList);
        Objects.requireNonNull(iCordaWritePayload).setSignature(payload.getRight());

        payLoadList.add(iCordaWritePayload);

        return payLoadList;
    }

    @Suspendable
    private ImmutablePair<List<Object>, String> createPayload(final CordaRPCOps proxy,
                                                              final ClientObject clientObject,
                                                              final ICordaWritePayload iCordaWritePayload) {
        List<String> notariesAsString = Helper.getNotariesAsStringList(proxy);

        CordaX500Name parse = CordaX500Name.parse(GenericSelectionStrategy.selectFixed(notariesAsString,
                Collections.singletonList(0), false).get(0));
        Party notary = proxy.notaryPartyFromX500Name(parse);

        List<Party> partyList = Helper.getPartiesWithoutNotaries(proxy);

        QueryCriteria.VaultQueryCriteria vaultQueryCriteria = new QueryCriteria.VaultQueryCriteria(
                Vault.StateStatus.UNCONSUMED, null, null, null, null,
                null, Vault.RelevancyStatus.ALL, Collections.EMPTY_SET, Collections.EMPTY_SET,
                null, Collections.EMPTY_LIST, null);
        PageSpecification pageSpecification = new PageSpecification(Configuration.DEFAULT_PAGE_NUMBER,
                Configuration.DEFAULT_PAGE_SIZE);
        //Sort sort = new Sort(Collections.emptyList());
        Sort.Attribute attribute = Sort.VaultStateAttribute.RECORDED_TIME;
        SortAttribute sortAttribute = new SortAttribute.Standard(attribute);
        Sort.SortColumn sortColumn = new Sort.SortColumn(sortAttribute, Sort.Direction.DESC);
        Sort sort = new Sort(Collections.singleton(sortColumn));

        String signature = clientObject.getClientId() + "---";
        String key =
                RandomStringUtils.random(10, true, true) + ((Math.random() * (1000000000 - 0)) + 0) + Math.random();

        List<Object> values = new ArrayList<>();
        values.add(KeyValueFlow.class);
        values.add("Set");
        List<String> params = new ArrayList<>();
        params.add(key);
        params.add(RandomStringUtils.random(Configuration.KEY_VALUE_STRING_LENGTH, true, true));
        params.add(signature);
        values.add(params);
        values.add(notary);

        for (final Party proxyNode : proxy.nodeInfo().getLegalIdentities()) {
            partyList.remove(proxyNode);
            LOG.info("Removed party (reflexive) " + proxyNode.toString());
        }

        values.add(partyList);
        //values.add(partyList.subList(2, 3));
        values.add(vaultQueryCriteria);
        values.add(pageSpecification);
        values.add(sort);

        iCordaWritePayload.setValueToRead(key);
        iCordaWritePayload.setEventPrefix("keyValue/set ");
        iCordaWritePayload.setSpecificPayloadType("keyValue");

        return ImmutablePair.of(values, signature);
    }

}
