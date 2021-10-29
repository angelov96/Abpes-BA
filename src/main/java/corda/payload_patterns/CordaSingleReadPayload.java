package corda.payload_patterns;

import client.client.ClientObject;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.keyvalueflow.states.KeyValueState;
import corda.configuration.Configuration;
import corda.payloads.ICordaReadPayload;
import net.corda.core.node.services.Vault;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import net.corda.core.node.services.vault.SortAttribute;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CordaSingleReadPayload implements ICordaPayloads {

    private static final Logger LOG = Logger.getLogger(CordaSingleReadPayload.class);

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> List<ICordaReadPayload> getPayloads(final E... params) {

        if (params.length != 1) {
            throw new IllegalArgumentException("Expecting exactly 1 argument for: " + this.getClass().getName());
        }

        return preparePayloads(params);
    }

    @Suspendable
    @SafeVarargs
    private final <E> List<ICordaReadPayload> preparePayloads(final E... values) {
        List<ICordaReadPayload> payLoadList = new ArrayList<>();

        ICordaReadPayload iCordaReadPayload = null;
        try {
            iCordaReadPayload = Configuration.READ_PAYLOAD.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }

        Object[] payload = createPayload((ClientObject) values[0]);

        Objects.requireNonNull(iCordaReadPayload).setValues(payload);

        iCordaReadPayload.setSpecificPayloadType("keyValue");

        payLoadList.add(iCordaReadPayload);

        return payLoadList;
    }

    @Suspendable
    private Object[] createPayload(final ClientObject clientObject) {

        QueryCriteria.VaultQueryCriteria vaultQueryCriteria =
                new QueryCriteria.VaultQueryCriteria(Vault.StateStatus.ALL);
        PageSpecification pageSpecification = new PageSpecification(Configuration.DEFAULT_PAGE_NUMBER,
                Configuration.DEFAULT_PAGE_SIZE);
        //Sort sort = new Sort(Collections.emptyList());
        Sort.Attribute attribute = Sort.VaultStateAttribute.RECORDED_TIME;
        SortAttribute sortAttribute = new SortAttribute.Standard(attribute);
        Sort.SortColumn sortColumn = new Sort.SortColumn(sortAttribute, Sort.Direction.DESC);
        Sort sort = new Sort(Collections.singleton(sortColumn));

        List<Object> values = new ArrayList<>();
        values.add(vaultQueryCriteria);
        values.add(pageSpecification);
        values.add(sort);
        values.add(KeyValueState.class);

        return values.toArray();
    }

}
