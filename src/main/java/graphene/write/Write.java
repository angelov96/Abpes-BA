package graphene.write;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketListener;
import graphene.components.Transaction;
import graphene.components.TransactionBroadcastSequence;
import graphene.components.TransactionBuilder;
import graphene.configuration.Configuration;
import graphene.connection.GrapheneWebsocket;
import graphene.listener.GrapheneWitnessListener;
import graphene.statistics.WriteStatisticObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class Write implements IWritingMethod {

    private static final Logger LOG = Logger.getLogger(Write.class);

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> ImmutablePair<Boolean, String> write(final E... params) {

        if (params.length == 5) {
            Transaction transaction = (Transaction) params[0];
            boolean oneTime = (Boolean) params[1];
            WebSocket webSocket = (WebSocket) params[2];
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[3];
            int walletOffset = (Integer) params[4];

            return write(transaction, oneTime, webSocket, writeStatisticObject, walletOffset);
        }
        throw new NotYetImplementedException("Not yet implemented function called");
    }

    @Suspendable
    private ImmutablePair<Boolean, String> write(final Transaction transaction, final boolean oneTime,
                                                 final WebSocket webSocket,
                                                 final WriteStatisticObject writeStatisticObject,
                                                 final int walletOffset) {

        writeStatisticObject.setStartTime(System.nanoTime());

        WebSocket webSocketWrite = null;
        try {
            TransactionBuilder transactionBuilder;
            TransactionBroadcastSequence transactionBroadcastSequence;
            if (Configuration.USE_TRANSACTION_BUILDER) {
                transactionBuilder = new TransactionBuilder(transaction, oneTime,
                        new GrapheneWitnessListener().registerWitnessResponseListener(), writeStatisticObject);
                List<WebSocketListener> webSocketListenerList = new ArrayList<>();
                webSocketListenerList.add(transactionBuilder);
                webSocketWrite = GrapheneWebsocket.connectToServer(webSocket, webSocketListenerList);
                ImmutablePair<Boolean, String> result;
                try {
                    result = transactionBuilder.getResponseFuture().get(Configuration.TIMEOUT_TRANSACTION,
                            Configuration.TIMEOUT_UNIT_TRANSACTION);
                } catch (TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    result = new ImmutablePair<>(true, "TIMEOUT_EX");
                }
                if (Configuration.SEND_WRITE_SYNC) {
                    LOG.info("Write result: " + result);
                    return result;
                } else if (Configuration.SEND_WRITE_ASYNC) {
                    LOG.debug("Sent async");
                    return result;
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }

            } else {
                transactionBroadcastSequence = new TransactionBroadcastSequence(transaction,
                        Configuration.FEE_ASSET_ID, oneTime,
                        new GrapheneWitnessListener().registerWitnessResponseListener(), writeStatisticObject,
                        walletOffset);
                List<WebSocketListener> webSocketListenerList = new ArrayList<>();
                webSocketListenerList.add(transactionBroadcastSequence);
                webSocketWrite = GrapheneWebsocket.connectToServer(webSocket, webSocketListenerList);
                ImmutablePair<Boolean, String> result;
                try {
                    result = transactionBroadcastSequence.getResponseFuture().get(Configuration.TIMEOUT_TRANSACTION,
                            Configuration.TIMEOUT_UNIT_TRANSACTION);
                } catch (TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    result = new ImmutablePair<>(true, "TIMEOUT_EX");
                }
                if (Configuration.SEND_WRITE_SYNC) {
                    LOG.info("Write result: " + result);
                    return result;
                } else if (Configuration.SEND_WRITE_ASYNC) {
                    LOG.debug("Sent async");
                    return result;
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }
            }
        } catch (/*todo specify concrete exception(s) ExecutionException | InterruptedException*/ Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        } finally {
            if (webSocketWrite != null) {
                if (Configuration.CLOSE_SOCKET_AFTER_WRITE) {
                    webSocketWrite.disconnect();
                }
            }
        }
    }

}
