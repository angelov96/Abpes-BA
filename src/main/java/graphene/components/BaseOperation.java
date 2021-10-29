package graphene.components;

import com.google.gson.*;
import cy.agorise.graphenej.AssetAmount;
import cy.agorise.graphenej.Extensions;
import cy.agorise.graphenej.interfaces.ByteSerializable;
import cy.agorise.graphenej.interfaces.JsonSerializable;
import cy.agorise.graphenej.operations.TransferOperation;
import graphene.payloads.OperationType;

import java.lang.reflect.Type;

/**
 * Base class that represents a generic operation
 */
public abstract class BaseOperation implements ByteSerializable, JsonSerializable {

    public static final String KEY_FEE = "fee";
    public static final String KEY_EXTENSIONS = "extensions";
    private final Extensions extensions;
    private OperationType type;

    public BaseOperation(final OperationType type) {
        this.type = type;
        this.extensions = new Extensions();
    }

    public void setType(final OperationType type) {
        this.type = type;
    }

    public Extensions getExtensions() {
        return extensions;
    }

    public abstract void setFee(AssetAmount assetAmount);

    public JsonElement toJsonObject() {
        JsonArray array = new JsonArray();
        array.add(this.getId());
        return array;
    }

    public byte getId() {
        return (byte) this.type.ordinal();
    }

    /**
     * <p>
     * De-serializer used to unpack data from a generic operation. The general format used in the
     * JSON-RPC blockchain API is the following:
     * </p>
     *
     * <code>[OPERATION_ID, OPERATION_OBJECT]</code><br>
     *
     * <p>
     * Where <code>OPERATION_ID</code> is one of the operations defined in {@link cy.agorise.graphenej.OperationType}
     * and <code>OPERATION_OBJECT</code> is the actual operation serialized in the JSON format.
     * </p>
     * Here's an example of this serialized form for a transfer operation:<br><br>
     * <pre>
     * [
     *   0,
     *   {
     *       "fee": {
     *           "amount": 264174,
     *           "asset_id": "1.3.0"
     *       },
     *       "from": "1.2.138632",
     *       "to": "1.2.129848",
     *       "amount": {
     *           "amount": 100,
     *           "asset_id": "1.3.0"
     *       },
     *       "extensions": []
     *   }
     * ]
     * </pre><br>
     * If this class is used, this serialized data will be translated to a TransferOperation object instance.<br>
     * <p>
     * TODO: Add support for operations other than the 'transfer'
     */
    public static class OperationDeserializer implements JsonDeserializer<BaseOperation> {

        @Override
        public BaseOperation deserialize(final JsonElement json, final Type typeOfT,
                                         final JsonDeserializationContext context) throws JsonParseException {
            BaseOperation operation = null;
            if (json.isJsonArray()) {
                JsonArray array = json.getAsJsonArray();
                if (array.get(0).getAsLong() == OperationType.TRANSFER_OPERATION.ordinal()) {
                    operation = context.deserialize(array.get(1), TransferOperation.class);
                }
            }
            return operation;
        }
    }
}
