package quorum.write;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import cy.agorise.graphenej.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.RawTransaction;
import org.web3j.crypto.TransactionEncoder;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.admin.Admin;
import org.web3j.protocol.admin.methods.response.NewAccountIdentifier;
import org.web3j.protocol.admin.methods.response.PersonalUnlockAccount;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.methods.response.*;
import org.web3j.protocol.websocket.WebSocketService;
import quorum.configuration.Configuration;
import quorum.helper.Helper;
import quorum.statistics.WriteStatisticObject;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public final class WriteHelper {

    private static final Logger LOG = Logger.getLogger(WriteHelper.class);

    private WriteHelper() {
    }

    @Suspendable
    public static synchronized String createNewAccount(final WebSocketService webSocketService,
                                          final String password) {

        Admin admin = Admin.build(webSocketService);

        Request<?, NewAccountIdentifier> newAccountIdentifierRequest = admin.personalNewAccount(password);
        try {
            NewAccountIdentifier newAccountIdentifier = newAccountIdentifierRequest.send();
            if (Configuration.DEBUG_SENT_TRANSACTION) {
                newAccountIdentifier = (NewAccountIdentifier) Helper.debugSend(newAccountIdentifier,
                        newAccountIdentifierRequest, "WriteHelper, createNewAccount", false);
            }

            LOG.debug("New account: " + newAccountIdentifier.getAccountId());
            return newAccountIdentifier.getAccountId();
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return "";
    }

    @Suspendable
    public static synchronized boolean unlockAccount(final WebSocketService webSocketService, final String addressToUnlock,
                                        final String password) {
        Admin admin = Admin.build(webSocketService);

        try {
            boolean unlockAccount = getPersonalUnlockAccount(addressToUnlock, password, admin);
            for (int i = 0; !unlockAccount && i < Configuration.LOGIN_RETRIES; i++) {
                unlockAccount = getPersonalUnlockAccount(addressToUnlock, password, admin);
            }
            return unlockAccount;
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return false;
    }

    @Suspendable
    private static boolean getPersonalUnlockAccount(final String addressToUnlock, final String password,
                                                    final Admin admin) throws IOException {
        Request<?, PersonalUnlockAccount> personalUnlockAccountRequest = admin.personalUnlockAccount(
                addressToUnlock, password, Configuration.DEFAULT_UNLOCK_DURATION);
        PersonalUnlockAccount unlockAccount = personalUnlockAccountRequest.send();
        if (Configuration.DEBUG_SENT_TRANSACTION) {
            unlockAccount = (PersonalUnlockAccount) Helper.debugSend(unlockAccount, personalUnlockAccountRequest,
                    "WriteHelper, getPersonalUnlockAccount", false);
        }
        return unlockAccount.accountUnlocked();
    }

    @Suspendable
    public static synchronized AtomicLong getNonce(final Web3j web3j, final String fromAddress) throws Exception {
        Request<?, EthGetTransactionCount> ethGetTransactionCountRequest = web3j.ethGetTransactionCount(
                fromAddress, Configuration.DEFAULT_BLOCK_PARAMETER);
        EthGetTransactionCount ethGetTransactionCount = ethGetTransactionCountRequest.send();
        if (Configuration.DEBUG_SENT_TRANSACTION) {
            ethGetTransactionCount = (EthGetTransactionCount) Helper.debugSend(ethGetTransactionCount,
                    ethGetTransactionCountRequest, "WriteHelper, getNonce", false);
        }

        LOG.debug(fromAddress + " Nonce | Transaction count: " + ethGetTransactionCount.getTransactionCount());
        return new AtomicLong(ethGetTransactionCount.getTransactionCount().longValue());
    }

    @Suspendable
    public static ImmutablePair<Boolean, String> prepareCheckReceipt(final Web3j web3j,
                                                                     final EthSendTransaction transactionResponseSync,
                                                                     final CompletableFuture<EthSendTransaction> transactionResponseAsync, final WriteStatisticObject writeStatisticObject) {
        if (Configuration.SEND_WRITE_ASYNC) {
            try {
                EthSendTransaction ethSendTransaction = transactionResponseAsync.get(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);
                if (Configuration.DEBUG_SENT_TRANSACTION) {
                    Helper.debugSend(ethSendTransaction, "WriteHelper, checkReceipt");
                }
                if (ethSendTransaction.hasError()) {
                    return ImmutablePair.of(true, ethSendTransaction.getError().getMessage());
                }
                return checkTransactionReceipt(web3j, ethSendTransaction, writeStatisticObject);
            } catch (InterruptedException | ExecutionException ex) {
                ExceptionHandler.logException(ex);
                return ImmutablePair.of(true, ex.getMessage());
            } catch (TimeoutException ex) {
                ExceptionHandler.logException(ex);
                return new ImmutablePair<>(true, "TIMEOUT_EX");
            }
        } else if (Configuration.SEND_WRITE_SYNC) {
            return checkTransactionReceipt(web3j, transactionResponseSync, writeStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented");
        }
    }

    @Suspendable
    private static ImmutablePair<Boolean, String> checkTransactionReceipt(final Web3j web3j,
                                                                          final EthSendTransaction transactionResponse,
                                                                          final WriteStatisticObject writeStatisticObject) {

        EthGetTransactionReceipt transactionReceipt = checkTransactionReceiptLogic(web3j,
                transactionResponse.getTransactionHash());

        if (transactionReceipt != null && transactionReceipt.getTransactionReceipt().isPresent()) {
            TransactionReceipt receivedTransactionReceipt =
                    transactionReceipt.getTransactionReceipt().get();
            LOG.info("Transaction hash of receipt: " + receivedTransactionReceipt.getTransactionHash());
            writeStatisticObject.setEndTime(System.nanoTime());
            return ImmutablePair.of(false, receivedTransactionReceipt.getTransactionHash());
        } else {
            LOG.error("No transaction receipt obtained");
            return ImmutablePair.of(true, "No transaction receipt obtained");
        }
    }

    @Suspendable
    private static EthGetTransactionReceipt checkTransactionReceiptLogic(final Web3j web3j,
                                                                         final String txId) {
        EthGetTransactionReceipt transactionReceipt = null;
        try {
            Request<?, EthGetTransactionReceipt> ethGetTransactionReceiptRequest =
                    web3j.ethGetTransactionReceipt(txId);
            transactionReceipt = ethGetTransactionReceiptRequest.send();
            if (Configuration.DEBUG_SENT_TRANSACTION) {
                transactionReceipt = (EthGetTransactionReceipt) Helper.debugSend(transactionReceipt,
                        ethGetTransactionReceiptRequest, "WriteHelper, " +
                                "checkTransactionReceiptLogic", false);
            }
            for (int i = 0; !transactionReceipt.getTransactionReceipt().isPresent() && i < Configuration.TRANSACTION_RECEIPT_RETRIES; i++) {
                Strand.sleep(Configuration.TRANSACTION_RECEIPT_SLEEP);
                LOG.trace("Slept...");
                Request<?, EthGetTransactionReceipt> ethGetTransactionReceiptRequestRetry =
                        web3j.ethGetTransactionReceipt(txId);
                transactionReceipt =
                        ethGetTransactionReceiptRequestRetry.send();

                /*TransactionReceiptProcessor pollingTransactionReceiptProcessor =
                        Helper.registerPollingTransactionReceiptProcessor(web3j,
                                Configuration.TRANSACTION_RECEIPT_WAIT, Configuration.TRANSACTION_RECEIPT_RETRIES);
                TransactionReceiptProcessor queuingTransactionReceiptProcessor =
                        Helper.registerQueuingTransactionReceiptProcessor(web3j,
                                Configuration.TRANSACTION_RECEIPT_RETRIES, Configuration.TRANSACTION_RECEIPT_WAIT);
                TransactionReceipt waitingTransactionReceiptProcessor =
                        Helper.registerWaitingTransactionProcessor(transactionReceiptProcessor,
                                transactionResponse.getTransactionHash());*/

                if (Configuration.DEBUG_SENT_TRANSACTION) {
                    transactionReceipt = (EthGetTransactionReceipt) Helper.debugSend(transactionReceipt,
                            ethGetTransactionReceiptRequest, "WriteHelper, " +
                                    "checkTransactionReceiptLogic resend", false);
                }
            }
        } catch (InterruptedException | IOException | SuspendExecution ex) {
            ExceptionHandler.logException(ex);
        }
        return transactionReceipt;
    }

    @Suspendable
    public static byte[] signTx(final Web3j web3j, final String fromAddress, final RawTransaction rawTransaction,
                                final Credentials credentials) throws IOException {
        byte[] signedMessage = new byte[0];
        if ((Configuration.SIGN_RAW_LOCAL && Configuration.SIGN_RAW_WEB3J) || (!Configuration.SIGN_RAW_LOCAL && !Configuration.SIGN_RAW_WEB3J)) {
            throw new NotYetImplementedException("Invalid signing configuration");
        }
        if (Configuration.SIGN_RAW_LOCAL) {
            signedMessage = TransactionEncoder.signMessage(rawTransaction, credentials);
        } else if (Configuration.SIGN_RAW_WEB3J) {
            Request<?, Web3Sha3> sha3Request =
                    web3j.web3Sha3(Util.bytesToHex(TransactionEncoder.encode(rawTransaction)));
            Web3Sha3 sha3 = sha3Request.send();
            if (Configuration.DEBUG_SENT_TRANSACTION) {
                sha3 = (Web3Sha3) Helper.debugSend(sha3, sha3Request, "WriteHelper, ethSha3", false);
            }
            Request<?, EthSign> ethSignRequest = web3j.ethSign(fromAddress, sha3.getResult());
            EthSign sign = ethSignRequest.send();
            if (Configuration.DEBUG_SENT_TRANSACTION) {
                sign = (EthSign) Helper.debugSend(sign, ethSignRequest, "WriteHelper, ethSign", false);
            }
            LOG.info("Signature of transaction: " + sign.getSignature());
            signedMessage = Util.hexToBytes(sign.getSignature());
        }
        return signedMessage;
    }

}
