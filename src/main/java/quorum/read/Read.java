package quorum.read;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.web3j.abi.datatypes.Type;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthCall;
import quorum.configuration.Configuration;
import quorum.helper.Helper;
import quorum.payloads.IQuorumReadPayload;
import quorum.statistics.ReadStatisticObject;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Read implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(Read.class);

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> read(final E... params) {
        if (params.length == 5) {
            String fromAddress = (String) params[0];
            String toAddress = (String) params[1];
            IQuorumReadPayload iQuorumReadPayload = (IQuorumReadPayload) params[2];
            Web3j web3j = (Web3j) params[3];
            ReadStatisticObject readStatisticObject =
                    (ReadStatisticObject) params[4];

            return readCall(fromAddress, toAddress, iQuorumReadPayload, web3j, readStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> readCall(final String fromAddress, final String toAddress,
                                                    final IQuorumReadPayload iQuorumReadPayload, final Web3j web3j,
                                                    final ReadStatisticObject readStatisticObject) {

        readStatisticObject.setStartTime(System.nanoTime());

        Transaction ethCallTransaction = Transaction.createEthCallTransaction(
                fromAddress,
                toAddress, iQuorumReadPayload.getPayloadAsString());

        EthCall transactionResponseSync = null;
        CompletableFuture<EthCall> transactionResponseAsync = null;
        if (Configuration.SEND_READ_ASYNC) {
            try {
                transactionResponseAsync =
                        sendTransactionAsync(web3j, ethCallTransaction);
            } catch (Exception ex) {
                ExceptionHandler.logException(ex);
                readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return ImmutablePair.of(true, ex.getMessage());
            }
        } else if (Configuration.SEND_READ_SYNC) {
            transactionResponseSync = sendTransaction(web3j, ethCallTransaction);
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }

        if (Configuration.RECEIVE_READ_REQUEST) {
            try {
                if (Configuration.DECODE_READ_DATA) {
                    if (Configuration.SEND_READ_ASYNC) {
                        List<Type<?>> types = ReadHelper.decodeReadCall(transactionResponseAsync.get(),
                                iQuorumReadPayload);
                        readStatisticObject.setEndTime(System.nanoTime());
                        return ImmutablePair.of(false, types.toString());
                    } else if (Configuration.SEND_READ_SYNC) {
                        List<Type<?>> types = ReadHelper.decodeReadCall(transactionResponseSync, iQuorumReadPayload);
                        readStatisticObject.setEndTime(System.nanoTime());
                        return ImmutablePair.of(false, types.toString());
                    }
                } else {
                    readStatisticObject.setEndTime(System.nanoTime());
                    return ImmutablePair.of(false, "");
                }
            } catch (ExecutionException | InterruptedException ex) {
                ExceptionHandler.logException(ex);
                readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return ImmutablePair.of(true, ex.getMessage());
            }
        } else {
            readStatisticObject.setEndTime(-1);
            return ImmutablePair.of(false, "");
        }
        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
        return ImmutablePair.of(true, "Reading process failed");
    }

    @Suspendable
    private CompletableFuture<EthCall> sendTransactionAsync(final Web3j web3j,
                                                            final Transaction transaction) throws Exception {
        try {
            return web3j.ethCall(transaction, Configuration.DEFAULT_BLOCK_PARAMETER).sendAsync();
        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    @Suspendable
    private EthCall sendTransaction(final Web3j web3j,
                                    final Transaction transaction) {
        try {
            Request<?, EthCall> sendRequest = web3j.ethCall(transaction, Configuration.DEFAULT_BLOCK_PARAMETER);
            EthCall send = sendRequest.send();
            if (Configuration.DEBUG_SENT_TRANSACTION) {
                send = (EthCall) Helper.debugSend(send, sendRequest, "Read, sendTransaction", false);
            }
            return send;
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        throw new IllegalStateException("Sending process failed");
    }
}
