package graphene.helper;

import client.supplements.ExceptionHandler;
import client.utils.GenericSelectionStrategy;
import co.paralleluniverse.fibers.Suspendable;
import com.google.common.collect.ImmutableMap;
import com.neovisionaries.ws.client.WebSocket;
import cy.agorise.graphenej.RPC;
import cy.agorise.graphenej.Util;
import cy.agorise.graphenej.Varint;
import cy.agorise.graphenej.models.ApiCall;
import graphene.components.Transaction;
import graphene.configuration.ApiId;
import graphene.configuration.Configuration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.log4j.Logger;
import org.bitcoinj.core.DumpedPrivateKey;
import org.bitcoinj.core.ECKey;
import org.bouncycastle.crypto.digests.SHA256Digest;
import org.hyperledger.fabric.sdk.helper.Utils;

import javax.net.ssl.X509TrustManager;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class Helper {

    private static final short TRANSACTION_ID_BYTES_SUBSTRING_PLAIN = 64;
    private static final short TRANSACTION_ID_BYTES_SUBSTRING_SHA_256 = 40;
    private static final Logger LOG = Logger.getLogger(Helper.class);

    private Helper() {
    }

    @Suspendable
    public static List<String> getAccounts(final boolean isWallet) {
        if (isWallet) {
            return Configuration.SERVER_LIST_WALLET;
        } else {
            return Configuration.SERVER_LIST;
        }
    }

    @Suspendable
    public static byte[] toBytesVarint(final long value) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteArrayOutputStream);
        try {
            Varint.writeUnsignedVarLong(value, out);
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Suspendable
    public static int byteArrayToLeInt(final byte[] encodedValue) {
        int value = (encodedValue[3] << (Byte.SIZE * 3));
        value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
        value |= (encodedValue[1] & 0xFF) << (Byte.SIZE);
        value |= (encodedValue[0] & 0xFF);
        return value;
    }

    @Suspendable
    public static void getTxId(final Transaction transaction) {
        String transactionBytesToHex = Util.bytesToHex(transaction.toBytes());
        LOG.debug("TransactionBytesToHex step 1: " + transactionBytesToHex);
        transactionBytesToHex = transactionBytesToHex.substring(TRANSACTION_ID_BYTES_SUBSTRING_PLAIN);
        LOG.debug("TransactionBytesToHex step 2: " + transactionBytesToHex);
        byte[] txWithoutSig = Util.hexToBytes(transactionBytesToHex);
        String txId = Util.bytesToHex(Utils.hash(txWithoutSig, new SHA256Digest()));
        LOG.debug("Computed TxId: " + txId);
        LOG.info("Computed TxId final: " + txId.substring(0, TRANSACTION_ID_BYTES_SUBSTRING_SHA_256));
    }

    @Suspendable
    public static byte[] leIntToByteArray(final int value) {
        byte[] encodedValue = new byte[Integer.BYTES];
        encodedValue[3] = (byte) (value >> Byte.SIZE * 3);
        encodedValue[2] = (byte) (value >> Byte.SIZE * 2);
        encodedValue[1] = (byte) (value >> Byte.SIZE);
        encodedValue[0] = (byte) value;
        return encodedValue;
    }

    @Suspendable
    public static void sendPasswordRequest(final WebSocket webSocket, final String password, final int id) {
        ArrayList<Serializable> passwordParams = new ArrayList<>();
        passwordParams.add(password);
        ApiCall setPasswordCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "set_password", passwordParams,
                RPC.VERSION, id);
        webSocket.sendText(setPasswordCall.toJsonString());
    }

    @Suspendable
    public static void sendUnlockCall(final WebSocket webSocket, final String password, final int id) {
        ArrayList<Serializable> passwordParams = new ArrayList<>();
        passwordParams.add(password);
        ApiCall unlockCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "unlock", passwordParams, RPC.VERSION, id);
        webSocket.sendText(unlockCall.toJsonString());
    }

    @Suspendable
    public static void sendImportKeyCall(final WebSocket webSocket, final String acctId, final String privateKey,
                                         final int id) {
        ArrayList<Serializable> accountParams = new ArrayList<>();
        accountParams.add(acctId);
        accountParams.add(privateKey);
        ApiCall importKeyCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "import_key", accountParams,
                RPC.VERSION, id);
        webSocket.sendText(importKeyCall.toJsonString());
    }

    @Suspendable
    public static void sendImportBalanceCall(final WebSocket webSocket, final String acctId, final String privateKey,
                                             final int id) {
        List<Serializable> balanceParams = new ArrayList<>();
        balanceParams.add(acctId);
        ArrayList<String> keyList = new ArrayList<>();
        keyList.add(privateKey);
        balanceParams.add(keyList);
        boolean broadcast = true;
        balanceParams.add(broadcast);
        ApiCall importBalanceCall = new ApiCall(ApiId.NON_RESTRICTED_API.ordinal(), "import_balance", balanceParams,
                RPC.VERSION, id);
        webSocket.sendText(importBalanceCall.toJsonString());
    }

    @Suspendable
    public static <K, V> Map<K, V> zipToMap(List<K> keys, List<V> values) {
        return IntStream.range(0, keys.size()).boxed()
                .collect(Collectors.toMap(keys::get, values::get));
    }

    @Suspendable
    public static ImmutableMap<String, List<ImmutablePair<String, String>>> getIpMap() {
        return ImmutableMap.copyOf(IP_MAP);
    }

    private static Map<String, List<ImmutablePair<String, String>>> IP_MAP;

    @Suspendable
    public static short getTransactionIdBytesSubstringPlain() {
        return TRANSACTION_ID_BYTES_SUBSTRING_PLAIN;
    }

    @Suspendable
    public static Map<String, List<ImmutablePair<String, String>>> readServerMapFromFile(final File file,
                                                                                                 final String accountIdPrefix, final int accountIdStart) {
        try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file))) {
            Map<String, List<ImmutablePair<String, String>>> ipMap = new LinkedHashMap<>();

            String line = lineNumberReader.readLine();
            while (line != null) {
                    String[] splitLineAddresses = line.split("\"");
                    if (splitLineAddresses.length == 13) {

                        String serverAddress = splitLineAddresses[12].replaceFirst(" ", "");
                        if(!ipMap.containsKey(serverAddress)) {
                            List<ImmutablePair<String, String>> keyAddressList = new ArrayList<>();
                            keyAddressList.add(new ImmutablePair<>(
                                    splitLineAddresses[3],
                                    accountIdPrefix + (accountIdStart + lineNumberReader.getLineNumber() - 1)));
                            ipMap.put(serverAddress, keyAddressList);
                        } else {
                            List<ImmutablePair<String, String>> keyAddressList = ipMap.get(serverAddress);
                            keyAddressList.add(new ImmutablePair<>(
                                    splitLineAddresses[3],
                                    accountIdPrefix + (accountIdStart + lineNumberReader.getLineNumber() - 1)));
                            ipMap.put(serverAddress, keyAddressList);
                        }

                    } else {
                        LOG.error("Unexpected length");
                    }
                line = lineNumberReader.readLine();
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
    public static List<ImmutableTriple<String, String, String>> readKeysServersAndAccountsFromFile(final File file,
                                                                                                   final int start,
                                                                                                   final int end,
                                                                                                   final String accountIdPrefix, final int accountIdStart) {
        try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file))) {
            List<ImmutableTriple<String, String, String>> keyServerAndAccountList = new ArrayList<>();

            String line = lineNumberReader.readLine();
            for (int i = 0; line != null; i++) {
                if (i >= start && i < end) {
                    String[] splitLineAddresses = line.split("\"");
                    if (splitLineAddresses.length == 13) {
                        keyServerAndAccountList.add(ImmutableTriple.of(splitLineAddresses[3],
                                splitLineAddresses[12].replaceFirst(" ", ""),
                                accountIdPrefix + (accountIdStart + lineNumberReader.getLineNumber() - 1)));
                    } else {
                        LOG.error("Unexpected length");
                    }
                } else {
                    LOG.debug(i + " not in range");
                }
                line = lineNumberReader.readLine();
                LOG.debug("Line: " + line);
            }

            for (final ImmutableTriple<String, String, String> key : keyServerAndAccountList) {
                LOG.info("Key, server and address: " + key);
            }

            return keyServerAndAccountList;
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return null;
    }

    @Suspendable
    public static List<ImmutablePair<String, String>> addAcctIdToPrivateKey(final String acctIdPrefix,
                                                                            final int acctIdStart,
                                                                            final List<String> keyList) {
        List<ImmutablePair<String, String>> acctIdWithPrivateKeyList = new ArrayList<>();
        for (int i = 0; i < keyList.size(); i++) {
            int currentAcctId = acctIdStart + i;
            acctIdWithPrivateKeyList.add(
                    ImmutablePair.of(acctIdPrefix + currentAcctId, keyList.get(i))
            );
        }
        return acctIdWithPrivateKeyList;
    }

    @Suspendable
    public static ECKey getEcKey(final String wif) {
        return DumpedPrivateKey.fromBase58(null, wif).getKey();
    }

    static class TrustEveryoneManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(final java.security.cert.X509Certificate[] chain, final String authType) {
        }

        @Override
        public void checkServerTrusted(final java.security.cert.X509Certificate[] chain, final String authType) {
        }

        @Override
        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }

    @Suspendable
    public static ImmutableTriple<String, ECKey, URI> getRandomAccountDataForServer(final String serverAddress) {
        List<ImmutablePair<String, String>> keyAndAccountList = Helper.getIpMap().get(serverAddress);
        List<ImmutablePair<String, String>> keyAndAccountPairList = GenericSelectionStrategy.selectRandom(keyAndAccountList,
                1, false);
        ImmutablePair<String, String> keyAndAccountPair = GenericSelectionStrategy.selectFixed(keyAndAccountPairList, Collections.singletonList(0), false).get(0);

        String privateKey = keyAndAccountPair.getLeft();
        String ecKeyForSourcePrivate = Objects.requireNonNull(privateKey); // Configuration
        // .ACCT_ID_PRIVATE_KEY_MAP.get(acctId);
        ECKey sourcePrivate = Helper.getEcKey(ecKeyForSourcePrivate);
        String acctId = keyAndAccountPair.getRight();

        URI uri = null;
        try {
            uri = new URI("ws://" + serverAddress);
        } catch (URISyntaxException ex) {
            ExceptionHandler.logException(ex);
        }

        LOG.info("(acctId | ECKey | URI):" +
                acctId + " | " + sourcePrivate.getPrivateKeyAsHex() + " | " + Objects.requireNonNull(uri));

        return ImmutableTriple.of(acctId, sourcePrivate, uri);

    }

}
