package sawtooth.endpointdata.singleblock;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class SingleBlock {

    @SerializedName("data")
    @Expose
    private Data data;
    @SerializedName("link")
    @Expose
    private String link;

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

}