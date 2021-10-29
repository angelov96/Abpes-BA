package graphene.listener;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.List;

public class BlockStructureWallet {

    @SerializedName("previous")
    @Expose
    private String previous;
    @SerializedName("timestamp")
    @Expose
    private String timestamp;
    @SerializedName("witness")
    @Expose
    private String witness;
    @SerializedName("transaction_merkle_root")
    @Expose
    private String transactionMerkleRoot;
    @SerializedName("extensions")
    @Expose
    private List<Object> extensions = null;
    @SerializedName("witness_signature")
    @Expose
    private String witnessSignature;
    @SerializedName("transactions")
    @Expose
    private List<TransactionStructureWallet> transactions = null;
    @SerializedName("block_id")
    @Expose
    private String blockId;
    @SerializedName("signing_key")
    @Expose
    private String signingKey;
    @SerializedName("transaction_ids")
    @Expose
    private List<String> transactionIds = null;

    public String getPrevious() {
        return previous;
    }

    public void setPrevious(String previous) {
        this.previous = previous;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getWitness() {
        return witness;
    }

    public void setWitness(String witness) {
        this.witness = witness;
    }

    public String getTransactionMerkleRoot() {
        return transactionMerkleRoot;
    }

    public void setTransactionMerkleRoot(String transactionMerkleRoot) {
        this.transactionMerkleRoot = transactionMerkleRoot;
    }

    public List<Object> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<Object> extensions) {
        this.extensions = extensions;
    }

    public String getWitnessSignature() {
        return witnessSignature;
    }

    public void setWitnessSignature(String witnessSignature) {
        this.witnessSignature = witnessSignature;
    }

    public List<TransactionStructureWallet> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<TransactionStructureWallet> transactions) {
        this.transactions = transactions;
    }

    public String getBlockId() {
        return blockId;
    }

    public void setBlockId(String blockId) {
        this.blockId = blockId;
    }

    public String getSigningKey() {
        return signingKey;
    }

    public void setSigningKey(String signingKey) {
        this.signingKey = signingKey;
    }

    public List<String> getTransactionIds() {
        return transactionIds;
    }

    public void setTransactionIds(List<String> transactionIds) {
        this.transactionIds = transactionIds;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("previous", previous).append("timestamp", timestamp).append("witness", witness).append("transactionMerkleRoot", transactionMerkleRoot).append("extensions", extensions).append("witnessSignature", witnessSignature).append("transactions", transactions).append("blockId", blockId).append("signingKey", signingKey).append("transactionIds", transactionIds).toString();
    }

}