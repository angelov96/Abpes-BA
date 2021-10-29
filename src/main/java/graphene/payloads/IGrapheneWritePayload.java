package graphene.payloads;

public interface IGrapheneWritePayload extends IGraphenePayload {

    Payload_Type PAYLOAD_TYPE = Payload_Type.WRITE;

    int getOperationId();

    String getSignature();

    void setSignature(String signature);

    void setBaseOperation(int ordinal);

    <E> E getValueToRead();

    <E> void setValueToRead(E valueToRead);

    String getEventPrefix();

    void setEventPrefix(String prefix);

}
