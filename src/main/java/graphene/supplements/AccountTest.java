package graphene.supplements;

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketListener;
import cy.agorise.graphenej.GrapheneObject;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.WitnessResponse;
import graphene.components.GetObjects;
import graphene.components.PublicKey;
import graphene.components.UserAccount;
import graphene.connection.GrapheneWebsocket;
import graphene.helper.Helper;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AccountTest {

    private static final String[] USER_IDS = new String[]{"1.2.6", "1.2.7", "1.2.8"};
    private static final Logger LOG = Logger.getLogger(AccountTest.class);
    private static final boolean IS_WALLET = false;

    private AccountTest() {
    }

    public static void main(final String... args) {


        Path currentRelativePath = Paths.get("");
        String currentAbsolutePath = currentRelativePath.toAbsolutePath().toString();
        String filePath = "/configs/";
        String logPropertiesName = "log4j.properties";

        PropertyConfigurator.configureAndWatch(currentAbsolutePath + filePath + logPropertiesName, 60 * 1000);

        for (final String server : Helper.getAccounts(IS_WALLET)) {
            WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                    server);

            List<String> ids = new ArrayList<>();
            for (final String userId : USER_IDS) {
                UserAccount userAccount = new UserAccount(userId);
                ids.add(userAccount.getObjectId());
            }
            List<WebSocketListener> webSocketListenerList = new ArrayList<>();
            webSocketListenerList.add(new GetObjects(ids, new WitnessResponseListener() {

                @Override
                public void onSuccess(final WitnessResponse response) {
                    List<GrapheneObject> result = (List<GrapheneObject>) response.result;
                    if (USER_IDS.length == result.size()) {
                        for (final GrapheneObject grapheneObject : result) {
                            UserAccount userAccount = (UserAccount) grapheneObject;
                            LOG.info("Account name.....: " + userAccount.getName());
                            LOG.info("json string......: " + userAccount.toJsonString());
                            if (!userAccount.getOwner().getKeyAuthList().isEmpty()) {
                                for (final PublicKey publicKey : userAccount.getOwner().getKeyAuthList()) {
                                    LOG.info("owner............: " + publicKey.getAddress());
                                }
                            }
                            if (!userAccount.getActive().getKeyAuthList().isEmpty()) {
                                for (final PublicKey publicKey : userAccount.getActive().getKeyAuthList()) {
                                    LOG.info("active key.......: " + publicKey.getAddress());
                                }
                            }
                            if (!userAccount.getActive().getAccountAuthList().isEmpty()) {
                                for (final UserAccount userAccountInAuthList :
                                        userAccount.getActive().getAccountAuthList()) {
                                    LOG.info("active account...: " + userAccountInAuthList.getObjectId());
                                }
                            }
                            if (userAccount.getOptions().getMemoKey() != null) {
                                LOG.info("memo.............: " + userAccount.getOptions().getMemoKey().getAddress());
                            }
                            webSocket.disconnect();
                        }
                    }
                }

                @Override
                public void onError(final BaseResponse.Error error) {
                    LOG.error("Error while checking accounts: " + error.message);
                    webSocket.disconnect();
                }
            }));

            GrapheneWebsocket.connectToServer(webSocket, webSocketListenerList);
        }

    }
}
