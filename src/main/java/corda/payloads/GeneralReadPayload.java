package corda.payloads;

import net.corda.core.contracts.ContractState;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;
import org.apache.log4j.Logger;

public class GeneralReadPayload implements ICordaReadPayload {

    private static final Logger LOG = Logger.getLogger(GeneralReadPayload.class);
    private QueryCriteria.VaultQueryCriteria vaultQueryCriteria;
    private PageSpecification pageSpecification;
    private Sort sort;
    private Class<? extends ContractState> clazz;

    @Override
    public QueryCriteria.VaultQueryCriteria getVaultQueryCriteria() {
        return vaultQueryCriteria;
    }

    public void setVaultQueryCriteria(final QueryCriteria.VaultQueryCriteria vaultQueryCriteria) {
        this.vaultQueryCriteria = vaultQueryCriteria;
    }

    @Override
    public PageSpecification getPageSpecification() {
        return pageSpecification;
    }

    public void setPageSpecification(final PageSpecification pageSpecification) {
        this.pageSpecification = pageSpecification;
    }

    @Override
    public Sort getSort() {
        return sort;
    }

    public void setSort(final Sort sort) {
        this.sort = sort;
    }

    @Override
    public Class<? extends ContractState> getClazz() {
        return clazz;
    }

    public void setClazz(final Class<? extends ContractState> clazz) {
        this.clazz = clazz;
    }

    @SafeVarargs
    @Override
    public final <E> E getPayload(final E... params) {
        return (E) this;
    }

    @SafeVarargs
    @Override
    public final <E> void setValues(final E... params) {
        vaultQueryCriteria = (QueryCriteria.VaultQueryCriteria) params[0];
        pageSpecification = (PageSpecification) params[1];
        sort = (Sort) params[2];
        clazz = (Class<? extends ContractState>) params[3];
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
