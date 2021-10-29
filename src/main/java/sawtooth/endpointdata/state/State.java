package sawtooth.endpointdata.state;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.util.List;

public class State {

    @SerializedName("data")
    @Expose
    private List<Datum> data = null;
    @SerializedName("head")
    @Expose
    private String head;
    @SerializedName("link")
    @Expose
    private String link;
    @SerializedName("paging")
    @Expose
    private Paging paging;

    public List<Datum> getData() {
        return data;
    }

    public void setData(List<Datum> data) {
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

    public Paging getPaging() {
        return paging;
    }

    public void setPaging(Paging paging) {
        this.paging = paging;
    }
}