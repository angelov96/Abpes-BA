package sawtooth.endpointdata.singleblock;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Header {

    @SerializedName("signer_public_key")
    @Expose
    private String signerPublicKey;
    @SerializedName("transaction_ids")
    @Expose
    private List<String> transactionIds = null;

    public String getSignerPublicKey() {
        return signerPublicKey;
    }

    public void setSignerPublicKey(String signerPublicKey) {
        this.signerPublicKey = signerPublicKey;
    }

    public List<String> getTransactionIds() {
        return transactionIds;
    }

    public void setTransactionIds(List<String> transactionIds) {
        this.transactionIds = transactionIds;
    }

}