package corda.payload_patterns.memory;

import client.client.ClientObject;
import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import com.memoryflow.flows.MemoryFlow;
import corda.configuration.Configuration;
import corda.helper.PartyMap;
import corda.payload_patterns.ICordaPayloads;
import corda.payloads.ICordaWritePayload;
import net.corda.core.identity.CordaX500Name;
import net.corda.core.identity.Party;
import net.corda.core.messaging.CordaRPCOps;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CordaUniformMemoryPayload implements ICordaPayloads {

    private static final Logger LOG = Logger.getLogger(CordaUniformMemoryPayload.class);

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

        String signature = System.currentTimeMillis() + RandomStringUtils.random(20, true, true);

        List<Object> values = new ArrayList<>();

        values.add(MemoryFlow.class);
        values.add("Memory");

        List<String> params = new ArrayList<>();
        params.add(String.valueOf(Configuration.LEN_OUTER_LOOP_MEMORY));
        params.add(String.valueOf(Configuration.LEN_INNER_LOOP_MEMORY));
        params.add(String.valueOf(Configuration.FIRST_CHAR_INT_MEMORY));
        params.add(String.valueOf(Configuration.LENGTH_MEMORY));
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

        iCordaWritePayload.setValueToRead(null);
        iCordaWritePayload.setEventPrefix("memory ");
        iCordaWritePayload.setSpecificPayloadType("memory");

        return ImmutablePair.of(values, signature);
    }

}
