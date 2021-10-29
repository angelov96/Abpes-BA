package sawtooth.payloads;

import java.util.List;

public interface ISawtoothWritePayload extends ISawtoothPayload {

    Payload_Type PAYLOAD_TYPE = Payload_Type.WRITE;

    <E> E getPlainAddressValues();

    List<String> getInputAddresses(String... params);

    List<String> getOutputAddresses(String... params);

    String getSignature();

    void setSignature(String signature);

    String getEventPrefix();

    void setEventPrefix(String prefix);

    <E> E getValueToRead();

    <E> void setValueToRead(E valueToRead);

    String getFamilyName();

    void setFamilyName(String familyName);

    String getFamilyVersion();

    void setFamilyVersion(String familyVersion);

}
