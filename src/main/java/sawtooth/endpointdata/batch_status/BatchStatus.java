package sawtooth.endpointdata.batch_status;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class BatchStatus {

    @SerializedName("data")
    @Expose
    private List<Datum> data = null;
    @SerializedName("link")
    @Expose
    private String link;

    public List<Datum> getData() {
        return data;
    }

    public void setData(List<Datum> data) {
        this.data = data;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

}