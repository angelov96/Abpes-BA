package sawtooth.endpointdata.blocks;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Transaction {

    @SerializedName("header")
    @Expose
    private Header_ header;
    @SerializedName("header_signature")
    @Expose
    private String headerSignature;
    @SerializedName("payload")
    @Expose
    private String payload;

    public Header_ getHeader() {
        return header;
    }

    public void setHeader(Header_ header) {
        this.header = header;
    }

    public String getHeaderSignature() {
        return headerSignature;
    }

    public void setHeaderSignature(String headerSignature) {
        this.headerSignature = headerSignature;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}