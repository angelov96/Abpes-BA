package sawtooth.endpointdata.transactions;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Datum {

    @SerializedName("header")
    @Expose
    private Header header;
    @SerializedName("header_signature")
    @Expose
    private String headerSignature;
    @SerializedName("payload")
    @Expose
    private String payload;

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

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

}