package graphene.components;

import com.google.gson.*;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketFrame;
import cy.agorise.graphenej.Asset;
import cy.agorise.graphenej.AssetAmount;
import cy.agorise.graphenej.GrapheneObject;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.api.BaseGrapheneHandler;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.*;
import graphene.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Class that implements get_objects request handler.
 * <p>
 * Get the objects corresponding to the provided IDs.
 * <p>
 * The response returns a list of objects retrieved, in the order they are mentioned in ids
 *
 * @see <a href="https://goo.gl/isRfeg">get_objects API doc</a>
 */
public class GetObjects extends BaseGrapheneHandler {

    private static final Logger LOG = Logger.getLogger(GetObjects.class);

    private final List<String> ids;

    private final boolean mOneTime;

    /**
     * Using this constructor the WebSocket connection closes after the response.
     *
     * @param ids      list of IDs of the objects to retrieve
     * @param listener A class implementing the WitnessResponseListener interface. This should
     *                 be implemented by the party interested in being notified about the
     *                 success/failure of the operation.
     */
    public GetObjects(final List<String> ids, final WitnessResponseListener listener) {
        this(ids, true, listener);
    }

    /**
     * Default Constructor
     *
     * @param ids      list of IDs of the objects to retrieve
     * @param oneTime  boolean value indicating if WebSocket must be closed (true) or not
     *                 (false) after the response
     * @param listener A class implementing the WitnessResponseListener interface. This should
     *                 be implemented by the party interested in being notified about the
     *                 success/failure of the operation.
     */
    public GetObjects(final List<String> ids, final boolean oneTime, final WitnessResponseListener listener) {
        super(listener);
        this.ids = ids;
        this.mOneTime = oneTime;
    }

    @Override
    public void onConnected(final WebSocket websocket, final Map<String, List<String>> headers) {
        ArrayList<Serializable> params = new ArrayList<>();
        ArrayList<Serializable> subParams = new ArrayList<>(this.ids);
        params.add(subParams);
        ApiCall apiCall = new ApiCall(0, RPC.CALL_GET_OBJECTS, params, RPC.VERSION, 0);
        websocket.sendText(apiCall.toJsonString());
    }

    @Override
    public void onTextFrame(final WebSocket websocket, final WebSocketFrame frame) throws Exception {
        if (frame.isTextFrame()) {
            for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                if (frame.getPayloadText().contains(errorMessage)) {
                    throw new Exception(frame.getPayloadText());
                }
            }
            LOG.info("GetObjectRequest received: " + frame.getPayloadText());
        }

        String response = frame.getPayloadText();
        GsonBuilder gsonBuilder = new GsonBuilder();

        gsonBuilder.registerTypeAdapter(BitAssetData.class, new BitAssetData.BitAssetDataDeserializer());
        gsonBuilder.registerTypeAdapter(AssetFeed.class, new AssetFeed.AssetFeedDeserializer());
        gsonBuilder.registerTypeAdapter(ReportedAssetFeed.class, new ReportedAssetFeed.ReportedAssetFeedDeserializer());
        gsonBuilder.registerTypeAdapter(AssetAmount.class, new AssetAmount.AssetAmountDeserializer());
        gsonBuilder.registerTypeAdapter(Asset.class, new Asset.AssetDeserializer());
        gsonBuilder.registerTypeAdapter(UserAccount.class, new UserAccount.UserAccountFullDeserializer());
        gsonBuilder.registerTypeAdapter(Authority.class, new Authority.AuthorityDeserializer());
        gsonBuilder.registerTypeAdapter(AccountOptions.class, new AccountOptions.AccountOptionsDeserializer());
        Gson gson = gsonBuilder.create();

        List<GrapheneObject> parsedResult = new ArrayList<>();

        JsonParser parser = new JsonParser();
        JsonArray resultArray =
                parser.parse(response).getAsJsonObject().get(WitnessResponse.KEY_RESULT).getAsJsonArray();
        for (int i = 0; i < resultArray.size(); i++) {
            JsonElement element = resultArray.get(i);
            String id = element.getAsJsonObject().get(GrapheneObject.KEY_ID).getAsString();
            LOG.info("Id: " + id);
            GrapheneObject grapheneObject = new GrapheneObject(id);
            if (grapheneObject.getObjectType() == null) {
                LOG.error("Unknown object type, maybe add this type if relevant");
            } else {
                switch (grapheneObject.getObjectType()) {
                    case ASSET_OBJECT:
                        Asset asset = gson.fromJson(element, Asset.class);
                        parsedResult.add(asset);
                        break;
                    case ACCOUNT_OBJECT:
                        UserAccount account = gson.fromJson(element, UserAccount.class);
                        parsedResult.add(account);
                        break;
                    case ASSET_BITASSET_DATA:
                        BitAssetData bitAssetData = gson.fromJson(element, BitAssetData.class);
                        parsedResult.add(bitAssetData);
                        break;
                }
            }

            WitnessResponse<List<GrapheneObject>> output = new WitnessResponse<>();
            output.result = parsedResult;
            mListener.onSuccess(output);
        }
        if (mOneTime) {
            websocket.disconnect();
        }
    }

    @Override
    public void onFrameSent(final WebSocket websocket, final WebSocketFrame frame) {
        if (frame.isTextFrame()) {
            System.out.println(">> " + frame.getPayloadText());
        }
    }

    @Override
    public void onError(final WebSocket websocket, final WebSocketException cause) {
        LOG.error("Error in GetObjects " + cause.getMessage());
    }
}
