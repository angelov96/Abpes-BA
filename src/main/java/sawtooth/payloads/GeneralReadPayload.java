package sawtooth.payloads;

import org.apache.log4j.Logger;

public class GeneralReadPayload implements ISawtoothReadPayload {

    private static final Logger LOG = Logger.getLogger(GeneralReadPayload.class);
    private String valueToRead;
    private TpEnum tpEnum;

    @Override
    public String getTpPrefix() {
        return tpPrefix;
    }

    @Override
    public void setTpPrefix(final String tpPrefix) {
        this.tpPrefix = tpPrefix;
    }

    private String tpPrefix;

    @Override
    public String getValueToRead() {
        return valueToRead;
    }

    @Override
    public void setValueToRead(final String valueToRead) {
        this.valueToRead = valueToRead;
    }

    @Override
    public TpEnum getTpEnum() {
        return tpEnum;
    }

    @Override
    public void setTpEnum(final TpEnum tpEnum) {
        this.tpEnum = tpEnum;
    }

    @SafeVarargs
    @Override
    public final <E> E getPayload(final E... params) {
        return (E) this;
    }

    @SafeVarargs
    @Override
    public final <E> void setValues(final E... params) {
        valueToRead = (String) params[0];
        tpEnum = (TpEnum) params[1];
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
