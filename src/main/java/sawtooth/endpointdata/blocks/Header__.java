package sawtooth.endpointdata.blocks;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Header__ {

    @SerializedName("batch_ids")
    @Expose
    private List<String> batchIds = null;
    @SerializedName("block_num")
    @Expose
    private String blockNum;
    @SerializedName("consensus")
    @Expose
    private String consensus;
    @SerializedName("previous_block_id")
    @Expose
    private String previousBlockId;
    @SerializedName("signer_public_key")
    @Expose
    private String signerPublicKey;
    @SerializedName("state_root_hash")
    @Expose
    private String stateRootHash;

    public List<String> getBatchIds() {
        return batchIds;
    }

    public void setBatchIds(List<String> batchIds) {
        this.batchIds = batchIds;
    }

    public String getBlockNum() {
        return blockNum;
    }

    public void setBlockNum(String blockNum) {
        this.blockNum = blockNum;
    }

    public String getConsensus() {
        return consensus;
    }

    public void setConsensus(String consensus) {
        this.consensus = consensus;
    }

    public String getPreviousBlockId() {
        return previousBlockId;
    }

    public void setPreviousBlockId(String previousBlockId) {
        this.previousBlockId = previousBlockId;
    }

    public String getSignerPublicKey() {
        return signerPublicKey;
    }

    public void setSignerPublicKey(String signerPublicKey) {
        this.signerPublicKey = signerPublicKey;
    }

    public String getStateRootHash() {
        return stateRootHash;
    }

    public void setStateRootHash(String stateRootHash) {
        this.stateRootHash = stateRootHash;
    }

}