package quorum.helper;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import cy.agorise.graphenej.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.log4j.Logger;
import org.bouncycastle.jcajce.provider.digest.Keccak;
import org.web3j.crypto.*;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.response.*;
import org.web3j.protocol.exceptions.TransactionException;
import org.web3j.protocol.websocket.WebSocketService;
import org.web3j.tx.response.Callback;
import org.web3j.tx.response.PollingTransactionReceiptProcessor;
import org.web3j.tx.response.QueuingTransactionReceiptProcessor;
import org.web3j.tx.response.TransactionReceiptProcessor;
import quorum.configuration.Configuration;
import quorum.write.WriteHelper;

import java.io.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class Helper {

    private static final Logger LOG = Logger.getLogger(Helper.class);

    private Helper() {
    }

    @Suspendable
    public static void calculateTxHash(final RawTransaction rawTransaction, final Credentials credentials) {
        if (Configuration.CALCULATE_TX_HASH) {
            String txHashGenerated = TransactionUtils.generateTransactionHashHexEncoded(rawTransaction, credentials);
            LOG.info("TxHash: " + txHashGenerated);
        }
    }

    @Suspendable
    public static void calculateTxHash(final byte[] signedMessage) {
        if (Configuration.CALCULATE_TX_HASH) {
            String txHash = calculateKeccak256(Util.bytesToHex(signedMessage));
            LOG.info("TxHash: " + txHash);
        }
    }

    @Suspendable
    private static String calculateKeccak256(final String signature) {
        Keccak.Digest256 digest256 = new Keccak.Digest256();
        byte[] hashBytes = digest256.digest(
                Util.hexToBytes(signature));
        String sha3hex = Util.bytesToHex(hashBytes);
        Log.info("Transaction hash: " + sha3hex);
        return "0x" + sha3hex;
    }

    @Suspendable
    public static String hexToASCII(final String hexValue) {
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < hexValue.length(); i += 2) {
            String str = hexValue.substring(i, i + 2);
            output.append((char) Integer.parseInt(str, 16));
        }
        return output.toString();
    }

    @Suspendable
    public static boolean debugSend(final Response<?> response, final String caller) {
        if (response.hasError()) {
            Response.Error error = response.getError();
            LOG.error("Error message: " + error.getMessage() + " " +
                    "Error data: " + error.getData() + " " +
                    "Error code: " + error.getCode() + " "
                    + "Response id: " + response.getId());
        }
        LOG.info("ID: " + response.getId() + " Raw response: " + response.getRawResponse() +
                " Result: " + response.getResult() + " " + caller);
        return response.hasError();
    }

    @Suspendable
    public static Response<?> debugSend(final Response<?> response,
                                        final Request<?, ?> request, final String caller,
                                        final boolean resendUponError) {
        Response<?> responseCopy = response;
        if (response.hasError()) {
            Response.Error error = response.getError();
            LOG.error("Error message: " + error.getMessage());
            LOG.error("Error data: " + error.getData());
            LOG.error("Error code: " + error.getCode());
            LOG.error("Response id: " + response.getId());

            if (resendUponError) {
                int resendCounter = Configuration.RESEND_TIMES_UPON_ERROR_WRITE;
                boolean hasError = true;
                for (int i = 0; hasError && i < resendCounter; i++) {
                    try {
                        LOG.error("Resent " + i + " times");
                        responseCopy = request.send();
                        hasError = false;
                    } catch (IOException ex) {
                        ExceptionHandler.logException(ex);
                        hasError = true;
                    }
                }
            }
        }
        LOG.info("ID: " + response.getId() + " Raw response: " + response.getRawResponse() + " Result: " + response.getResult() + " " + caller);
        return responseCopy;
    }

    @Suspendable
    public static void getNetworkStats(final Web3j web3j) {
        try {
            NetPeerCount peerCount = web3j.netPeerCount().send();
            Web3ClientVersion clientVersion = web3j.web3ClientVersion().send();
            EthSyncing ethSyncing = web3j.ethSyncing().send();
            EthMining ethMining = web3j.ethMining().send();
            EthGetWork ethGetWork = web3j.ethGetWork().send();
            EthAccounts ethAccounts = web3j.ethAccounts().send();
            EthHashrate ethHashrate = web3j.ethHashrate().send();
            EthGasPrice ethGasPrice = web3j.ethGasPrice().send();
            LOG.info("JsonRPCVersion:" + peerCount.getJsonrpc());
            LOG.info("Web3ClientVersion:" + clientVersion.getWeb3ClientVersion());
            LOG.info("Number of peers: " + peerCount.getQuantity() + "\n" +
                    "Is syncing: " + ethSyncing.isSyncing() + "\n" +
                    "Is mining: " + ethMining.isMining() + "\n" +
                    "Hashrate: " + ethHashrate.getHashrate() + "\n" +
                    "Gasprice: " + ethGasPrice.getGasPrice() + "\n" +
                    "Work: " +
                    (!ethGetWork.hasError() ?
                            ethGetWork.getBoundaryCondition() : ethGetWork.getError().getMessage()) +
                    " / " +
                    (!ethGetWork.hasError() ?
                            ethGetWork.getCurrentBlockHeaderPowHash() :
                            ethGetWork.getError().getMessage()));
            // boundary 2^256 / difficulty
            for (final String account : ethAccounts.getAccounts()) {
                LOG.info("Account: " + account);
            }
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    public static TransactionReceiptProcessor registerQueuingTransactionReceiptProcessor(final Web3j web3j,
                                                                                         final int pollingAttemptsPerTxHash,
                                                                                         final long pollingFrequencyInMs) {
        return
                new QueuingTransactionReceiptProcessor(web3j, new Callback() {
                    @Override
                    @Suspendable
                    public void accept(final TransactionReceipt transactionReceipt) {
                        LOG.info("Received txId " + transactionReceipt.getTransactionHash());
                    }

                    @Override
                    @Suspendable
                    public void exception(final Exception ex) {
                        ExceptionHandler.logException(ex, false);
                    }
                }, pollingAttemptsPerTxHash, pollingFrequencyInMs);
    }

    @Suspendable
    public static TransactionReceiptProcessor registerPollingTransactionReceiptProcessor(final Web3j web3j,
                                                                                         final long sleepDuration,
                                                                                         final int attempts) {
        return new PollingTransactionReceiptProcessor(web3j, sleepDuration, attempts);
    }

    @Suspendable
    public static TransactionReceipt registerWaitingTransactionProcessor(final TransactionReceiptProcessor transactionReceiptProcessor, final String txHash) {
        try {
            return transactionReceiptProcessor.waitForTransactionReceipt(txHash);
        } catch (IOException | TransactionException ex) {
            ExceptionHandler.logException(ex, false);
        }
        return null;
    }

    @Suspendable
    public static Credentials getWalletCredentials(final String password, final String walletInformation) {
        try {
            return WalletUtils.loadCredentials(password, walletInformation);
        } catch (CipherException | IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return null;
    }

    public static ImmutableMap<String, List<ImmutablePair<String, String>>> getIpMap() {
        return ImmutableMap.copyOf(IP_MAP);
    }

    private static Map<String, List<ImmutablePair<String, String>>> IP_MAP;

    @Suspendable
    public static Map<String, List<ImmutablePair<String, String>>> readToIpMapFromFile(final File file) {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {

            Map<String, List<ImmutablePair<String, String>>> ipMap = new LinkedHashMap<>();
            
            String line = bufferedReader.readLine();
            for (int i = 0; line != null; i++) {
                    String[] splitLine = line.split(" ");
                    if (splitLine.length == 4) {

                        if (splitLine[0].length() > 0 && isValidJson(splitLine[0])) {
                            writeWalletFiles(splitLine[0], i);
                        } else {
                            if ("\"\"".equals(splitLine[0])) {
                                splitLine[0] = "";
                            } else if (!splitLine[0].startsWith("0x")) {
                                splitLine[0] = "0x" + splitLine[0];
                            }
                        }
                        if ("\"\"".equals(splitLine[1])) {
                            splitLine[1] = "";
                        }
                        if ("\"\"".equals(splitLine[2])) {
                            splitLine[2] = "";
                        }
                        if ("\"\"".equals(splitLine[3])) {
                            splitLine[3] = "";
                        }

                        String serverAddress = "http://" + splitLine[2] + ":" + splitLine[3];

                        if(!ipMap.containsKey(serverAddress)) {
                            List<ImmutablePair<String, String>> addressPasswordPairList = new ArrayList<>();
                            addressPasswordPairList.add(ImmutablePair.of(splitLine[0], splitLine[1]));
                            ipMap.put(serverAddress, addressPasswordPairList);
                        } else {
                            List<ImmutablePair<String, String>> immutablePairs = ipMap.get(serverAddress);
                            immutablePairs.add(ImmutablePair.of(splitLine[0], splitLine[1]));
                        }

                    } else {
                        LOG.error("Unexpected length: " + splitLine.length);
                    }
                line = bufferedReader.readLine();
                LOG.debug("Line: " + line);
            }

            for (final Map.Entry<String, List<ImmutablePair<String, String>>> stringListEntry : ipMap.entrySet()) {
               LOG.debug("IP/port: " + stringListEntry.getKey() + " address/passwordlist " + stringListEntry.getValue().toString());
            }

            IP_MAP = ipMap;
            return ipMap;
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return null;
    }

    @Suspendable
    public static List<ImmutableTriple<String, String, String>> readAddressesFromFile(final File file,
                                                                                      final int start, final int end) {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
            List<ImmutableTriple<String, String, String>> addressesPasswordsAndServers = new ArrayList<>();

            String line = bufferedReader.readLine();
            for (int i = 0; line != null; i++) {
                if (i >= start && i < end) {
                    String[] splitLine = line.split(" ");
                    if (splitLine.length == 4) {

                        if (splitLine[0].length() > 0 && isValidJson(splitLine[0])) {
                            writeWalletFiles(splitLine[0], i);
                        } else {
                            if ("\"\"".equals(splitLine[0])) {
                                splitLine[0] = "";
                            } else if (!splitLine[0].startsWith("0x")) {
                                splitLine[0] = "0x" + splitLine[0];
                            }
                        }
                        if ("\"\"".equals(splitLine[1])) {
                            splitLine[1] = "";
                        }
                        if ("\"\"".equals(splitLine[2])) {
                            splitLine[2] = "";
                        }
                        if ("\"\"".equals(splitLine[3])) {
                            splitLine[3] = "";
                        }

                        String serverAddress = "http://" + splitLine[2] + ":" + splitLine[3];

                        addressesPasswordsAndServers.add(ImmutableTriple.of(splitLine[0], splitLine[1], serverAddress));
                    } else {
                        LOG.error("Unexpected length: " + splitLine.length);
                    }
                } else {
                    LOG.debug(i + " not in range");
                }
                line = bufferedReader.readLine();
                LOG.debug("Line: " + line);
            }

            for (final ImmutableTriple<String, String, String> addressesPasswordsAndServersTmp :
                    addressesPasswordsAndServers) {
                LOG.debug("Content: " + addressesPasswordsAndServersTmp.getLeft() + " password: " + addressesPasswordsAndServersTmp.getMiddle()
                        + " server address: " + addressesPasswordsAndServersTmp.getRight() + " | Start: "
                        + start + " End: " + end);
            }

            return addressesPasswordsAndServers;
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return null;
    }

    @Suspendable
    public static boolean isValidJson(final String jsonString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonString);
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    @Suspendable
    public static void writeWalletFiles(final String walletString, final int counter) {
        try {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(
                    Configuration.WALLET_PATH_PREFIX + counter + Configuration.WALLET_ENDING))) {
                writer.write(walletString);
            }
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

    @Suspendable
    public static void unlock(final WebSocketService webSocketService, final String fromAddress,
                              final String fromPassword) {
        boolean unlocked = WriteHelper.unlockAccount(webSocketService,
                fromAddress, fromPassword);

        LOG.info(fromAddress + " got unlocked successfully: " + unlocked);
    }

    @Suspendable
    public static synchronized AtomicLong setNonceFor(final Web3j web3j, final String fromAddress) {

        AtomicLong nonce = null;
        try {
            nonce =
                    WriteHelper.getNonce(web3j,
                            fromAddress);

            if (Configuration.DISTRIBUTED_NONCE_HANDLING) {

                try (java.sql.Connection connection = client.database.Connection.getConnection()) {
                    String query = "INSERT INTO quorum_nonce AS qn (address, nonce)" +
                            "VALUES (?, ?)" +
                            "ON CONFLICT (address) DO NOTHING " +
                            "RETURNING nonce";

                    PreparedStatement preparedStatement = connection.prepareStatement(query);
                    preparedStatement.setString(1, fromAddress);
                    preparedStatement.setLong(2, nonce.get());

                    ResultSet resultSet = preparedStatement.executeQuery();

                    int i = 0;
                    while (resultSet.next()) {
                        LOG.info("Current nonce: " + resultSet.getInt("nonce"));
                        i++;
                    }
                    if (i == 0) {
                        LOG.info("Result set was empty");
                    }

                    resultSet.close();
                    preparedStatement.close();
                } catch (SQLException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
        } catch (Exception ex) {
            ExceptionHandler.logException(ex);
        }
        return nonce;
    }

}