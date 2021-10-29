package sawtooth.endpointdata.singlebatch;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Data {

    @SerializedName("header")
    @Expose
    private Header header;
    @SerializedName("header_signature")
    @Expose
    private String headerSignature;
    @SerializedName("trace")
    @Expose
    private boolean trace;
    @SerializedName("transactions")
    @Expose
    private List<Transaction> transactions = null;

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public String getHeaderSignature() {
        return headerSignature;
    }

    public void setHeaderSignature(String headerSignature) {
        this.headerSignature = headerSignature;
    }

    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }

}