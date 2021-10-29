package quorum.payloads;

public interface IQuorumWritePayload extends IQuorumPayload {

    Payload_Type PAYLOAD_TYPE = Payload_Type.WRITE;

    String getSignature();

    void setSignature(String signature);

    <E> E getValueToRead();

    <E> void setValueToRead(E valueToRead);

    String getEventPrefix();

    void setEventPrefix(String prefix);

}
