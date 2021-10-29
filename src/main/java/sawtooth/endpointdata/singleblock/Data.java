package sawtooth.endpointdata.singleblock;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class Data {

    @SerializedName("batches")
    @Expose
    private List<Batch> batches = null;
    @SerializedName("header")
    @Expose
    private Header__ header;
    @SerializedName("header_signature")
    @Expose
    private String headerSignature;

    public List<Batch> getBatches() {
        return batches;
    }

    public void setBatches(List<Batch> batches) {
        this.batches = batches;
    }

    public Header__ getHeader() {
        return header;
    }

    public void setHeader(Header__ header) {
        this.header = header;
    }

    public String getHeaderSignature() {
        return headerSignature;
    }

    public void setHeaderSignature(String headerSignature) {
        this.headerSignature = headerSignature;
    }
}