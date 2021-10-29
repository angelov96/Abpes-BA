package graphene.payloads;

import com.google.common.primitives.Bytes;
import com.google.gson.*;
import cy.agorise.graphenej.AssetAmount;
import graphene.components.BaseOperation;
import graphene.components.UserAccount;
import graphene.helper.Helper;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class GeneralPayload extends BaseOperation implements IGrapheneWritePayload {

    private static final Logger LOG = Logger.getLogger(GeneralPayload.class);
//    private static final byte[] AUXILIARY_EVENT = "".getBytes();
    private String keyAccount;
    private String keyFee;
    private String keyFunction;
    private String keyParameters;
    private int operationType;
    private AssetAmount fee;
    private UserAccount account;
    private String Function;
    private List<String> parameters;
    private String signature;
    private String valueToRead;
    private String prefix;

    public GeneralPayload(final int operationType, final UserAccount accountConstructor,
                          final AssetAmount feeConstructor, final String functionConstructor,
                          final List<String> parametersConstructor) {
        super(OperationType.values()[operationType]);

        account = accountConstructor;
        if (feeConstructor != null) {
            fee = feeConstructor;
        }
        Function = functionConstructor;
        parameters = parametersConstructor;
    }

    public GeneralPayload() {
        super(null);
    }

    public String getKeyAccount() {
        return keyAccount;
    }

    public void setKeyAccount(final String keyAccount) {
        this.keyAccount = keyAccount;
    }

    public String getKeyFee() {
        return keyFee;
    }

    public void setKeyFee(final String keyFee) {
        this.keyFee = keyFee;
    }

    public String getKeyFunction() {
        return keyFunction;
    }

    public void setKeyFunction(final String keyFunction) {
        this.keyFunction = keyFunction;
    }

    public String getKeyParameters() {
        return keyParameters;
    }

    public void setKeyParameters(final String keyParameters) {
        this.keyParameters = keyParameters;
    }

    public int getOperationType() {
        return operationType;
    }

    public void setOperationType(final int operationType) {
        this.operationType = operationType;
    }

    public UserAccount getAccount() {
        return account;
    }

    public void setAccount(final UserAccount account) {
        this.account = account;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public void setParameters(final List<String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public int getOperationId() {
        return operationType;
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public void setSignature(final String signature) {
        this.signature = signature;
    }

    @SafeVarargs
    @Override
    public final <E> void setValues(final E... params) {
        setOperationType((Integer) params[0]);
        setKeyAccount((String) params[1]);
        setAccount((UserAccount) params[2]);
        setKeyFee((String) params[3]);
        setFee((AssetAmount) params[4]);
        setKeyFunction((String) params[5]);
        setFunction((String) params[6]);
        setKeyParameters((String) params[7]);
        setParameters((List<String>) params[8]);
    }

    public void setBaseOperation(final int operationType) {
        super.setType(OperationType.values()[operationType]);
    }

    @Override
    public <E> E getValueToRead() {
        return (E) valueToRead;
    }

    @Override
    public <E> void setValueToRead(final E valueToRead) {
        this.valueToRead = (String) valueToRead;
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

    public AssetAmount getFee() {
        return this.fee;
    }

    public void setFee(AssetAmount newFee) {
        this.fee = newFee;
    }

    @Override
    public byte[] toBytes() {
        byte[] accountBytes = account.toBytes();
        byte[] feeBytes = new byte[0];
        if (fee != null) {
            feeBytes = fee.toBytes();
        }
        byte[] functionBytes = Function.getBytes();
        byte[] parametersBytes =
                preparePayloadAsString(parameters).getBytes();

        byte[] varintFunction = Helper.toBytesVarint(getFunction().length());
        byte[] varintParameters = Helper.toBytesVarint(preparePayloadAsString(parameters).length());
        /*byte[] varintAux = Helper.toBytesVarint(AUXILIARY_EVENT.length);*/

        return Bytes.concat(feeBytes, accountBytes, varintFunction, functionBytes, varintParameters,
                parametersBytes/*, varintAux, AUXILIARY_EVENT*/);
    }

    public String getFunction() {
        return Function;
    }

    public void setFunction(final String function) {
        Function = function;
    }

    private String preparePayloadAsString(final List<String> parameters) {
        return parameters.toString().replace("[", "").replace("]", "")
                .replace(",", ";").replace(" ", "");
    }

    public String toJsonString() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(GeneralPayload.class, new GeneralPayloadDeserializer());
        return gsonBuilder.create().toJson(this);
    }

    public JsonElement toJsonObject() {
        JsonArray array = new JsonArray();
        array.add((byte) operationType);
        JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty(keyAccount, this.account.getObjectId());

        if (this.fee != null) {
            jsonObject.add(keyFee, this.fee.toJsonObject());
        }

        jsonObject.add(keyFunction, new JsonPrimitive(getFunction()));

        jsonObject.add(keyParameters, new JsonPrimitive(preparePayloadAsString(parameters)));

        array.add(jsonObject);

        return array;
    }

    @SafeVarargs
    @Override
    public final <E> E getPayload(final E... params) {
        return (E) this;
    }

    public static class GeneralPayloadSerializer implements JsonSerializer<GeneralPayload> {
        @Override
        public JsonElement serialize(final GeneralPayload generalPayload, final Type type,
                                     final JsonSerializationContext jsonSerializationContext) {
            return generalPayload.toJsonObject();
        }
    }

    public class GeneralPayloadDeserializer implements JsonDeserializer<GeneralPayload> {

        @Override
        public GeneralPayload deserialize(final JsonElement json, final Type typeOfT,
                                          final JsonDeserializationContext context) throws JsonParseException {
            if (json.isJsonArray()) {
                JsonArray serializedTransfer = json.getAsJsonArray();
                if (serializedTransfer.get(0).getAsInt() != operationType) {
                    LOG.debug("Got: " + serializedTransfer.get(0).getAsInt() + " != " + operationType);
                    return null;
                } else {
                    return context.deserialize(serializedTransfer.get(1), GeneralPayload.class);
                }
            } else {
                JsonObject jsonObject = json.getAsJsonObject();

                GsonBuilder builder = new GsonBuilder();
                builder.registerTypeAdapter(AssetAmount.class, new AssetAmount.AssetAmountDeserializer());
                Gson gson = builder.create();

                String function = jsonObject.get(keyFunction).getAsString();
                AssetAmount fee = null;
                if (keyFee != null) {
                    fee = gson.fromJson(jsonObject.get(KEY_FEE), AssetAmount.class);
                }
                UserAccount account = new UserAccount(jsonObject.get(keyAccount).getAsString());

                JsonArray asJsonArray = jsonObject.getAsJsonArray(keyParameters);
                List<String> params = new ArrayList<>();
                for (final JsonElement param : asJsonArray) {
                    params.add(param.getAsString());
                }

                return new GeneralPayload(operationType, account, fee, function, params);
            }
        }
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
