package corda.payloads;

import net.corda.core.identity.Party;

import java.util.List;

public interface ICordaWritePayload extends ICordaPayload {

    Payload_Type PAYLOAD_TYPE = Payload_Type.WRITE;

    String getSignature();

    void setSignature(String signature);

    <E> E getValueToRead();

    <E> void setValueToRead(E valueToRead);

    String getEventPrefix();

    void setEventPrefix(String prefix);

    List<Party> getParties();

    Party getNotary();


}
