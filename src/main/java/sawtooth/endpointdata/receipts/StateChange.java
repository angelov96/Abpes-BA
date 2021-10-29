package sawtooth.endpointdata.receipts;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class StateChange {

    @SerializedName("address")
    @Expose
    private String address;
    @SerializedName("type")
    @Expose
    private String type;
    @SerializedName("value")
    @Expose
    private String value;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

}