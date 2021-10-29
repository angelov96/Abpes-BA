package sawtooth.endpointdata.singleblock;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Header_ {

    @SerializedName("batcher_public_key")
    @Expose
    private String batcherPublicKey;
    @SerializedName("dependencies")
    @Expose
    private List<Object> dependencies = null;
    @SerializedName("family_name")
    @Expose
    private String familyName;
    @SerializedName("family_version")
    @Expose
    private String familyVersion;
    @SerializedName("inputs")
    @Expose
    private List<String> inputs = null;
    @SerializedName("nonce")
    @Expose
    private String nonce;
    @SerializedName("outputs")
    @Expose
    private List<String> outputs = null;
    @SerializedName("payload_sha512")
    @Expose
    private String payloadSha512;
    @SerializedName("signer_public_key")
    @Expose
    private String signerPublicKey;

    public String getBatcherPublicKey() {
        return batcherPublicKey;
    }

    public void setBatcherPublicKey(String batcherPublicKey) {
        this.batcherPublicKey = batcherPublicKey;
    }

    public List<Object> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<Object> dependencies) {
        this.dependencies = dependencies;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public String getFamilyVersion() {
        return familyVersion;
    }

    public void setFamilyVersion(String familyVersion) {
        this.familyVersion = familyVersion;
    }

    public List<String> getInputs() {
        return inputs;
    }

    public void setInputs(List<String> inputs) {
        this.inputs = inputs;
    }

    public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    public List<String> getOutputs() {
        return outputs;
    }

    public void setOutputs(List<String> outputs) {
        this.outputs = outputs;
    }

    public String getPayloadSha512() {
        return payloadSha512;
    }

    public void setPayloadSha512(String payloadSha512) {
        this.payloadSha512 = payloadSha512;
    }

    public String getSignerPublicKey() {
        return signerPublicKey;
    }

    public void setSignerPublicKey(String signerPublicKey) {
        this.signerPublicKey = signerPublicKey;
    }

}