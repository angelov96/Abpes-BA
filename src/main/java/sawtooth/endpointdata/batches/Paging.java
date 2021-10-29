package sawtooth.endpointdata.batches;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Paging {

    @SerializedName("limit")
    @Expose
    private Object limit;
    @SerializedName("next")
    @Expose
    private String next;
    @SerializedName("next_position")
    @Expose
    private String nextPosition;
    @SerializedName("start")
    @Expose
    private Object start;

    public Object getLimit() {
        return limit;
    }

    public void setLimit(Object limit) {
        this.limit = limit;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }

    public String getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(String nextPosition) {
        this.nextPosition = nextPosition;
    }

    public Object getStart() {
        return start;
    }

    public void setStart(Object start) {
        this.start = start;
    }

}