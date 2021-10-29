package sawtooth.endpointdata.receipts;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import sawtooth.sdk.protobuf.StateChange;

import java.util.List;

public class Datum {

    @SerializedName("data")
    @Expose
    private List<Object> data = null;
    @SerializedName("events")
    @Expose
    private List<Object> events = null;
    @SerializedName("id")
    @Expose
    private String id;
    @SerializedName("state_changes")
    @Expose
    private List<StateChange> stateChanges = null;

    public List<Object> getData() {
        return data;
    }

    public void setData(List<Object> data) {
        this.data = data;
    }

    public List<Object> getEvents() {
        return events;
    }

    public void setEvents(List<Object> events) {
        this.events = events;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<StateChange> getStateChanges() {
        return stateChanges;
    }

    public void setStateChanges(List<StateChange> stateChanges) {
        this.stateChanges = stateChanges;
    }

}