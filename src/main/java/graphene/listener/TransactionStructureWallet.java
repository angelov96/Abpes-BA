package graphene.listener;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.List;

public class TransactionStructureWallet {

    @SerializedName("ref_block_num")
    @Expose
    private Integer refBlockNum;
    @SerializedName("ref_block_prefix")
    @Expose
    private Long refBlockPrefix;
    @SerializedName("expiration")
    @Expose
    private String expiration;
    @SerializedName("operations")
    @Expose
    private List<List<?>> operations = null;
    @SerializedName("extensions")
    @Expose
    private List<Object> extensions = null;
    @SerializedName("signatures")
    @Expose
    private List<String> signatures = null;
    @SerializedName("operation_results")
    @Expose
    private List<List<?>> operationResults = null;

    public Integer getRefBlockNum() {
        return refBlockNum;
    }

    public void setRefBlockNum(Integer refBlockNum) {
        this.refBlockNum = refBlockNum;
    }

    public Long getRefBlockPrefix() {
        return refBlockPrefix;
    }

    public void setRefBlockPrefix(Long refBlockPrefix) {
        this.refBlockPrefix = refBlockPrefix;
    }

    public String getExpiration() {
        return expiration;
    }

    public void setExpiration(String expiration) {
        this.expiration = expiration;
    }

    public List<List<?>> getOperations() {
        return operations;
    }

    public void setOperations(List<List<?>> operations) {
        this.operations = operations;
    }

    public List<Object> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<Object> extensions) {
        this.extensions = extensions;
    }

    public List<String> getSignatures() {
        return signatures;
    }

    public void setSignatures(List<String> signatures) {
        this.signatures = signatures;
    }

    public List<List<?>> getOperationResults() {
        return operationResults;
    }

    public void setOperationResults(List<List<?>> operationResults) {
        this.operationResults = operationResults;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("refBlockNum", refBlockNum).append("refBlockPrefix", refBlockPrefix).append("expiration", expiration).append("operations", operations).append("extensions", extensions).append("signatures", signatures).append("operationResults", operationResults).toString();
    }

}