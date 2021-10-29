package sawtooth.endpointdata.singlestate;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class SingleState {

    @SerializedName("data")
    @Expose
    private String data;
    @SerializedName("head")
    @Expose
    private String head;
    @SerializedName("link")
    @Expose
    private String link;

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getHead() {
        return head;
    }

    public void setHead(String head) {
        this.head = head;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

}