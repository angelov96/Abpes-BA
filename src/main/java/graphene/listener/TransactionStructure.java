package graphene.listener;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class TransactionStructure {

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
    private List<?> extensions = null;
    @SerializedName("signatures")
    @Expose
    private List<String> signatures = null;

    public Integer getRefBlockNum() {
        return refBlockNum;
    }

    public void setRefBlockNum(final Integer refBlockNum) {
        this.refBlockNum = refBlockNum;
    }

    public Long getRefBlockPrefix() {
        return refBlockPrefix;
    }

    public void setRefBlockPrefix(final Long refBlockPrefix) {
        this.refBlockPrefix = refBlockPrefix;
    }

    @Override
    public String toString() {
        return "TransactionStructure{" +
                "refBlockNum=" + refBlockNum +
                ", refBlockPrefix=" + refBlockPrefix +
                ", expiration='" + expiration + '\'' +
                ", operations=" + operations +
                ", extensions=" + extensions +
                ", signatures=" + signatures +
                '}';
    }

    public String getExpiration() {
        return expiration;
    }

    public void setExpiration(final String expiration) {
        this.expiration = expiration;
    }

    public List<List<?>> getOperations() {
        return operations;
    }

    public void setOperations(final List<List<?>> operations) {
        this.operations = operations;
    }

    public List<?> getExtensions() {
        return extensions;
    }

    public void setExtensions(final List<?> extensions) {
        this.extensions = extensions;
    }

    public List<String> getSignatures() {
        return signatures;
    }

    public void setSignatures(final List<String> signatures) {
        this.signatures = signatures;
    }

}
