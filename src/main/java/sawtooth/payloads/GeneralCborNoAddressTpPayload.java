package sawtooth.payloads;

import client.supplements.ExceptionHandler;
import co.nstant.in.cbor.CborBuilder;
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborEncoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.builder.ArrayBuilder;
import co.nstant.in.cbor.builder.MapBuilder;
import co.nstant.in.cbor.model.DataItem;
import org.apache.log4j.Logger;
import org.spongycastle.util.encoders.DecoderException;
import sawtooth.configuration.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class GeneralCborNoAddressTpPayload implements ISawtoothWritePayload {

    private static final Logger LOG = Logger.getLogger(GeneralCborNoAddressTpPayload.class);
    private String function;
    private String[] parameters;
    private String signature;
    private String valueToRead;
    private String prefix;

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public void setSignature(final String signature) {
        this.signature = signature;
    }

    @Override
    public <E> E getValueToRead() {
        return (E) valueToRead;
    }

    @Override
    public <E> void setValueToRead(final E valueToRead) {
        this.valueToRead = (String) valueToRead;
    }

    private String familyName;
    private String familyVersion;

    @Override
    public String getFamilyName() {
        return familyName;
    }

    @Override
    public void setFamilyName(final String familyName) {
        this.familyName = familyName;
    }

    @Override
    public String getFamilyVersion() {
        return familyVersion;
    }

    @Override
    public void setFamilyVersion(final String familyVersion) {
        this.familyVersion = familyVersion;
    }

    @SafeVarargs
    public final <E> E getPayload(final E... params) {

        ArrayBuilder<MapBuilder<CborBuilder>> cborBuilderIntermediate = new CborBuilder()
                .addMap()
                .put("Function", getFunction())
                .startArray("Args");

        for (final String param : getPlainAddressValues()) {
            cborBuilderIntermediate.add(param);
        }

        List<DataItem> cbor = cborBuilderIntermediate.end().end().build();

        return (E) preparePayload(cbor);
    }

    public static ByteArrayOutputStream preparePayload(final List<DataItem> dataToEncode) {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        try {
            new CborEncoder(payload).encode(dataToEncode);
            if (Configuration.ENABLE_DEBUGGING && Configuration.DECODE_DATA_AS_CBOR) {

                ByteArrayInputStream byteArrayInputStream =
                        new ByteArrayInputStream(payload.toByteArray());
                List<DataItem> decode =
                        new CborDecoder(byteArrayInputStream).decode();
                byteArrayInputStream.close();
                //CborDecoder.decode(payload.toByteArray());

                for (final DataItem dataItem : decode) {
                    LOG.debug("DataItem: " + dataItem);
                }
            } else {
                LOG.debug("Not debugging payload");
            }
        } catch (CborException | DecoderException ex) {
            LOG.error("Not able to decode data");
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return payload;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(final String function) {
        this.function = function;
    }

    @Override
    public String[] getPlainAddressValues() {
        return parameters;
    }

    @SafeVarargs
    public final <E> void setValues(final E... params) {
        setFunction(String.valueOf(params[0]));
        setParameters((String[]) params[1]);
    }

    public void setParameters(final String[] parameters) {
        this.parameters = parameters;
    }

    @Override
    public List<String> getInputAddresses(final String... params) {
        return TpNoAddressHandler.getTpInputWrite(Configuration.TP_ENUM, Configuration.TP_PREFIX,
                Arrays.asList(params));
    }

    @Override
    public List<String> getOutputAddresses(final String... params) {
        return TpNoAddressHandler.getTpOutputWrite(Configuration.TP_ENUM, Configuration.TP_PREFIX,
                Arrays.asList(params));
    }

    @Override
    public String getEventPrefix() {
        if (prefix == null) {
            LOG.debug("Prefix is null");
            return "";
        }
        return prefix;
    }

    @Override
    public void setEventPrefix(final String prefix) {
        this.prefix = prefix;
    }

    private String specificPayloadType;

    @Override
    public String getSpecificPayloadType() {
        return specificPayloadType;
    }

    @Override
    public void setSpecificPayloadType(final String specificPayloadType) {
        this.specificPayloadType = specificPayloadType;
    }
}
