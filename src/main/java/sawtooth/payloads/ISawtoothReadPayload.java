package sawtooth.payloads;

public interface ISawtoothReadPayload extends ISawtoothPayload {

    Payload_Type PAYLOAD_TYPE = Payload_Type.READ;

    void setTpPrefix(String tpPrefix);

    String getValueToRead();

    String getTpPrefix();

    void setValueToRead(String valueToRead);

    TpEnum getTpEnum();

    void setTpEnum(TpEnum tpEnum);
}
