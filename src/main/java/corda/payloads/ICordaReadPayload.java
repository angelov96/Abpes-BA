package corda.payloads;

import net.corda.core.contracts.ContractState;
import net.corda.core.node.services.vault.PageSpecification;
import net.corda.core.node.services.vault.QueryCriteria;
import net.corda.core.node.services.vault.Sort;

public interface ICordaReadPayload extends ICordaPayload {

    Payload_Type PAYLOAD_TYPE = Payload_Type.READ;

    QueryCriteria.VaultQueryCriteria getVaultQueryCriteria();

    PageSpecification getPageSpecification();

    Sort getSort();

    Class<? extends ContractState> getClazz();

}
