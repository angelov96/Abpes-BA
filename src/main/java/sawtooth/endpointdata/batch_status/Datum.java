package sawtooth.endpointdata.batch_status;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Datum {

    @SerializedName("id")
    @Expose
    private String id;
    @SerializedName("invalid_transactions")
    @Expose
    private List<Object> invalidTransactions = null;
    @SerializedName("status")
    @Expose
    private String status;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<Object> getInvalidTransactions() {
        return invalidTransactions;
    }

    public void setInvalidTransactions(List<Object> invalidTransactions) {
        this.invalidTransactions = invalidTransactions;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}