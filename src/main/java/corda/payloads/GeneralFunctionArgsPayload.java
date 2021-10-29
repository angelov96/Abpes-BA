package corda.payloads;

import net.corda.core.flows.FlowLogic;
import net.corda.core.identity.Party;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.util.ArrayList;
import java.util.List;

public class GeneralFunctionArgsPayload implements ICordaWritePayload {

    private static final Logger LOG = Logger.getLogger(GeneralFunctionArgsPayload.class);

    private String function;
    private List<String> parameters;
    private String signature;
    private Party notary;
    private QueryCriteria.VaultQueryCriteria vaultQueryCriteria;
    private Sort sort;
    private PageSpecification pageSpecification;
    private Class<FlowLogic<?>> flowClass;
    private List<Party> parties;
    private String valueToRead;
    private String prefix;

    @SafeVarargs
    @Override
    public final <E> E getPayload(final E... params) {
        List<Object> paramList = new ArrayList<>();
        if(function != null) {
            paramList.add(getFunction());
        }
        paramList.add(getParameters());
        paramList.add(getNotary());
        paramList.add(getParties());
        if(getVaultQueryCriteria() != null) {
            paramList.add(getVaultQueryCriteria());
        }
        if(getPageSpecification() != null) {
            paramList.add(getPageSpecification());
        }
        if (getSort() != null) {
            paramList.add(getSort());
        }

        return (E) new ImmutablePair<>(getFlowClass(), paramList);
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(final String function) {
        this.function = function;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(final List<String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public Party getNotary() {
        return notary;
    }

    public void setNotary(final Party notary) {
        this.notary = notary;
    }

    public QueryCriteria.VaultQueryCriteria getVaultQueryCriteria() {
        return vaultQueryCriteria;
    }

    public void setVaultQueryCriteria(final QueryCriteria.VaultQueryCriteria vaultQueryCriteria) {
        this.vaultQueryCriteria = vaultQueryCriteria;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(final Sort sort) {
        this.sort = sort;
    }

    public PageSpecification getPageSpecification() {
        return pageSpecification;
    }

    public void setPageSpecification(final PageSpecification pageSpecification) {
        this.pageSpecification = pageSpecification;
    }

    public Class<FlowLogic<?>> getFlowClass() {
        return flowClass;
    }

    public void setFlowClass(final Class<FlowLogic<?>> flowClass) {
        this.flowClass = flowClass;
    }

    @Override
    public List<Party> getParties() {
        return parties;
    }

    public void setParties(final List<Party> parties) {
        this.parties = parties;
    }

    @SafeVarargs
    @Override
    public final <E> void setValues(final E... params) {

        if (params.length == 1) {
            List<Object> values = (List<Object>) params[0];

            setFlowClass((Class<FlowLogic<?>>) values.get(0));
            if(values.get(1) != null) {
                setFunction(String.valueOf(values.get(1)));
            }
            setParameters((List<String>) values.get(2));
            setNotary((Party) values.get(3));
            setParties((List<Party>) values.get(4));
            if(values.size() >= 6) {
                setVaultQueryCriteria((QueryCriteria.VaultQueryCriteria) values.get(5));
            }
            if(values.size() >= 7) {
                setPageSpecification((PageSpecification) values.get(6));
            }
            if(values.size() >= 8) {
                setSort((Sort) values.get(7));
            }
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }

    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public void setSignature(final String signature) {
        this.signature = signature;
    }

    @Override
    public <E> E getValueToRead() {
        return (E) valueToRead;
    }

    @Override
    public <E> void setValueToRead(final E valueToRead) {
        this.valueToRead = (String) valueToRead;
    }

    @Override
    public String getEventPrefix() {
        if (prefix == null) {
            LOG.debug("Prefix is null");
            return "";
        }
        return prefix;
    }

    @Override
    public void setEventPrefix(final String prefix) {
        this.prefix = prefix;
    }

    private String specificPayloadType;

    @Override
    public String getSpecificPayloadType() {
        return specificPayloadType;
    }

    @Override
    public void setSpecificPayloadType(final String specificPayloadType) {
        this.specificPayloadType = specificPayloadType;
    }
}
