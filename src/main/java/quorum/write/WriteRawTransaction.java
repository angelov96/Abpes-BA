package quorum.write;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.TransactionDecoder;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthCall;
import org.web3j.protocol.core.methods.response.EthSendTransaction;
import org.web3j.utils.Numeric;
import quorum.configuration.Configuration;
import quorum.helper.Helper;
import quorum.payloads.IQuorumWritePayload;
import quorum.statistics.WriteStatisticObject;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class WriteRawTransaction implements IWritingMethod {

    private static final Logger LOG = Logger.getLogger(WriteRawTransaction.class);

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> write(final E... params) {
        if (params.length == 7) {

            IQuorumWritePayload payload = (IQuorumWritePayload) params[0];
            Web3j web3j = (Web3j) params[1];
            Credentials credentials = (Credentials) params[2];
            String fromAddress = (String) params[3];
            String toAddress = (String) params[4];
            AtomicLong nonce = (AtomicLong) params[5];
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[6];

            return writeWithoutValue(payload, web3j, credentials, fromAddress, toAddress, nonce,
                    writeStatisticObject);
        } else if (params.length == 8) {

            IQuorumWritePayload payload = (IQuorumWritePayload) params[0];
            Web3j web3j = (Web3j) params[1];
            Credentials credentials = (Credentials) params[2];
            String fromAddress = (String) params[3];
            String toAddress = (String) params[4];
            BigInteger value = (BigInteger) params[5];
            AtomicLong nonce = (AtomicLong) params[6];
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[7];

            return writeWithValue(payload, web3j, credentials, fromAddress, toAddress,
                    value, nonce, writeStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> writeWithValue(final IQuorumWritePayload payload, final Web3j web3j,
                                                          final Credentials credentials, final String fromAddress,
                                                          final String toAddress,
                                                          final BigInteger value, final AtomicLong nonce,
                                                          final WriteStatisticObject writeStatisticObject) {

        writeStatisticObject.setStartTime(System.nanoTime());

        try {

            long nonceValue;
            if (Configuration.DISTRIBUTED_NONCE_HANDLING) {
                nonceValue = nonce.get();
            } else {
                nonceValue = nonce.getAndIncrement();
            }

            RawTransaction rawTransaction =
                    RawTransaction.createTransaction(BigInteger.valueOf(nonceValue),
                            Configuration.DEFAULT_GAS_PRICE, Configuration.DEFAULT_GAS_LIMIT, toAddress,
                            value, payload.getPayloadAsString());
            LOG.info("Nonce used: " + rawTransaction.getNonce() + " from: " + fromAddress);

            byte[] signedMessage;
            try {
                signedMessage = WriteHelper.signTx(web3j, fromAddress, rawTransaction, credentials);
            } catch (IOException ex) {
                ExceptionHandler.logException(ex);
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return ImmutablePair.of(true, ex.getMessage());
            }

            Helper.calculateTxHash(rawTransaction, credentials);
            String hexTransaction = Numeric.toHexString(signedMessage);

            EthSendTransaction transactionResponseSync = null;
            CompletableFuture<EthSendTransaction> transactionResponseAsync = null;
            if (Configuration.SEND_WRITE_ASYNC) {
                transactionResponseAsync =
                        sendTransactionAsync(web3j, hexTransaction);
                if (!Configuration.MEASURE_BY_RECEIPT) {
                    LOG.debug("Sent async");
                    writeStatisticObject.setEndTime(-1);
                    return ImmutablePair.of(false, "");
                }
            } else if (Configuration.SEND_WRITE_SYNC) {
                try {
                    transactionResponseSync = sendTransaction(web3j, hexTransaction, fromAddress);
                } catch (IOException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true, ex.getMessage());
                }
                if (transactionResponseSync.hasError()) {
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true, transactionResponseSync.getError().getMessage());
                }
                if (!Configuration.MEASURE_BY_RECEIPT) {
                    writeStatisticObject.setEndTime(System.nanoTime());
                    return ImmutablePair.of(false, transactionResponseSync.getTransactionHash());
                }
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }

            ImmutablePair<Boolean, String> receipt = WriteHelper.prepareCheckReceipt(web3j,
                    transactionResponseSync,
                    transactionResponseAsync, writeStatisticObject);
            if (receipt.getLeft()) {
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return receipt;
            }
            return receipt;
        } catch (/*todo specify concrete exception(s)*/ Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> writeWithoutValue(final IQuorumWritePayload payload, final Web3j web3j,
                                                             final Credentials credentials, final String fromAddress,
                                                             final String toAddress, final AtomicLong nonce,
                                                             final WriteStatisticObject writeStatisticObject) {

        writeStatisticObject.setStartTime(System.nanoTime());

        try {
            long nonceValue;
            if (Configuration.DISTRIBUTED_NONCE_HANDLING) {
                nonceValue = nonce.get();
            } else {
                nonceValue = nonce.getAndIncrement();
            }

            RawTransaction rawTransaction =
                    RawTransaction.createTransaction(BigInteger.valueOf(nonceValue),
                            Configuration.DEFAULT_GAS_PRICE, Configuration.DEFAULT_GAS_LIMIT, toAddress,
                            payload.getPayloadAsString());
            LOG.info("Nonce used: " + rawTransaction.getNonce() + " from: " + fromAddress);

            byte[] signedMessage;
            try {
                signedMessage = WriteHelper.signTx(web3j, fromAddress, rawTransaction, credentials);
            } catch (IOException ex) {
                ExceptionHandler.logException(ex);
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return ImmutablePair.of(true, ex.getMessage());
            }

            Helper.calculateTxHash(rawTransaction, credentials);
            String hexTransaction = Numeric.toHexString(signedMessage);

            EthSendTransaction transactionResponseSync = null;
            CompletableFuture<EthSendTransaction> transactionResponseAsync = null;
            if (Configuration.SEND_WRITE_ASYNC) {
                transactionResponseAsync =
                        sendTransactionAsync(web3j, hexTransaction);
                if (!Configuration.MEASURE_BY_RECEIPT) {
                    LOG.debug("Sent async");
                    writeStatisticObject.setEndTime(-1);
                    return ImmutablePair.of(false, "");
                }
            } else if (Configuration.SEND_WRITE_SYNC) {
                try {
                    transactionResponseSync = sendTransaction(web3j, hexTransaction, fromAddress);
                } catch (IOException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true, ex.getMessage());
                }
                if (transactionResponseSync.hasError()) {
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true, transactionResponseSync.getError().getMessage());
                }
                if (!Configuration.MEASURE_BY_RECEIPT) {
                    writeStatisticObject.setEndTime(System.nanoTime());
                    return ImmutablePair.of(false, transactionResponseSync.getTransactionHash());
                }
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }

            ImmutablePair<Boolean, String> receipt = WriteHelper.prepareCheckReceipt(web3j,
                    transactionResponseSync,
                    transactionResponseAsync, writeStatisticObject);
            if (receipt.getLeft()) {
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return receipt;
            }

            return receipt;
        } catch (/*todo specify concrete exception(s)*/ Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

    @Suspendable
    private CompletableFuture<EthSendTransaction> sendTransactionAsync(final Web3j web3j,
                                                                       final String rawTransaction) throws Exception {
        try {
            return web3j.ethSendRawTransaction(rawTransaction).sendAsync();
        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }

    @Suspendable
    private EthSendTransaction sendTransaction(final Web3j web3j,
                                               final String rawTransaction, final String fromAddress) throws Exception {

        Request<?, EthSendTransaction> ethSendTransactionRequest = web3j.ethSendRawTransaction(rawTransaction);
        EthSendTransaction send = ethSendTransactionRequest.send();
        if (Configuration.DEBUG_SENT_TRANSACTION) {
            send = (EthSendTransaction) Helper.debugSend(send, ethSendTransactionRequest, "WriteRawTransaction, " +
                    "sendTransaction", false);
        }
        if (Configuration.DIRECT_READ_AFTER_WRITE) {
            RawTransaction decode = TransactionDecoder.decode(rawTransaction);
            Request<?, EthCall> ethCallRequest = web3j.ethCall(new Transaction(fromAddress, decode.getNonce(),
                            decode.getGasPrice(),
                            decode.getGasLimit(), decode.getTo(), decode.getValue(), decode.getData()),
                    Configuration.DEFAULT_BLOCK_PARAMETER);
            EthCall read = ethCallRequest.send();
            LOG.info("Call : " + read.getValue());
        }
        return send;
    }

}
