package graphene.components;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import com.google.common.primitives.Bytes;
import com.google.gson.*;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketListener;
import cy.agorise.graphenej.*;
import cy.agorise.graphenej.interfaces.ByteSerializable;
import cy.agorise.graphenej.interfaces.JsonSerializable;
import cy.agorise.graphenej.operations.CustomOperation;
import cy.agorise.graphenej.operations.LimitOrderCreateOperation;
import cy.agorise.graphenej.operations.TransferOperation;
import graphene.configuration.Configuration;
import graphene.connection.GrapheneWebsocket;
import graphene.listener.GrapheneWitnessListener;
import graphene.payloads.OperationType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.bitcoinj.core.DumpedPrivateKey;
import org.bitcoinj.core.ECKey;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Utils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Type;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Class used to represent a generic Graphene transaction.
 */
public class Transaction implements ByteSerializable, JsonSerializable {

    /* Default expiration time */
    public static final int DEFAULT_EXPIRATION_TIME = 30;
    /* Constant field names used for serialization/deserialization purposes */
    public static final String KEY_EXPIRATION = "expiration";
    public static final String KEY_SIGNATURES = "signatures";
    public static final String KEY_OPERATIONS = "operations";
    public static final String KEY_EXTENSIONS = "extensions";
    public static final String KEY_REF_BLOCK_NUM = "ref_block_num";
    public static final String KEY_REF_BLOCK_PREFIX = "ref_block_prefix";
    private static final Logger LOG = Logger.getLogger(Transaction.class);
    private static final String DEFAULT_STRING_FOR_NEW_TRANSACTION = "";
    private final List<graphene.components.BaseOperation> operations;
    // Using the bitshares mainnet chain id by default
    private byte[] chainId = Util.hexToBytes(Chains.BITSHARES.CHAIN_ID);
    private ECKey privateKey;
    private BlockData blockData;
    private Extensions extensions;
    private byte[] signature;

    /**
     * Transaction constructor.
     *
     * @param wif            The user's private key in the base58 format.
     * @param block_data     Block data containing important information used to sign a transaction.
     * @param operation_list List of operations to include in the transaction.
     */
    public Transaction(final String wif, final BlockData block_data,
                       final List<graphene.components.BaseOperation> operation_list) {
        this(DumpedPrivateKey.fromBase58(null, wif).getKey(), block_data, operation_list);
    }

    /**
     * Transaction constructor.
     *
     * @param privateKey    Instance of a ECKey containing the private key that will be used to sign this transaction.
     * @param blockData     Block data containing important information used to sign a transaction.
     * @param operationList List of operations to include in the transaction.
     */
    public Transaction(final ECKey privateKey, final BlockData blockData,
                       final List<graphene.components.BaseOperation> operationList) {
        this(Util.hexToBytes(Chains.BITSHARES.CHAIN_ID), privateKey, blockData, operationList);
    }

    /**
     * Transaction constructor
     *
     * @param chainId    The chain id
     * @param privateKey Private key used to sign this transaction
     * @param blockData  Block data
     * @param operations List of operations contained in this transaction
     */
    public Transaction(final byte[] chainId, final ECKey privateKey, final BlockData blockData,
                       final List<graphene.components.BaseOperation> operations) {
        this.chainId = chainId;
        this.privateKey = privateKey;
        this.blockData = blockData;
        this.operations = operations;
        this.extensions = new Extensions();
    }

    /**
     * Constructor used to build a Transaction object without a private key. This kind of object
     * is used to represent a transaction data that we don't intend to serialize and sign.
     *
     * @param blockData     Block data instance, containing information about the location of this transaction in the
     *                      blockchain.
     * @param operationList The list of operations included in this transaction.
     */
    public Transaction(final BlockData blockData, final List<graphene.components.BaseOperation> operationList) {
        this.blockData = blockData;
        this.operations = operationList;
    }

    /**
     * Block data setter
     *
     * @return BlockData instance
     */
    public BlockData getBlockData() {
        return this.blockData;
    }

    /**
     * Block data getter
     *
     * @param blockData New block data
     */
    public void setBlockData(final BlockData blockData) {
        this.blockData = blockData;
    }

    /**
     * Updates the fees for all operations in this transaction.
     *
     * @param fees: New fees to apply
     */
    public void setFees(final List<AssetAmount> fees) {
        for (int i = 0; i < operations.size(); i++) {
            operations.get(i).setFee(fees.get(i));
        }
    }

    public ECKey getPrivateKey() {
        return this.privateKey;
    }

    public List<graphene.components.BaseOperation> getOperations() {
        return this.operations;
    }

    /**
     * This method is used to query whether the instance has a private key.
     *
     * @return
     */
    public boolean hasPrivateKey() {
        return this.privateKey != null;
    }

    @Suspendable
    public byte[] computeGrapheneSignature(final String txAsJsonString) {
        return computeSignature(txAsJsonString);
    }

    /**
     * Obtains a signature of this transaction. Please note that due to the current reliance on
     * bitcoinj to generate the signatures, and due to the fact that it uses deterministic
     * ecdsa signatures, we are slightly modifying the expiration time of the transaction while
     * we look for a signature that will be accepted by the graphene network.
     * <p>
     * This should then be called before any other serialization method.
     *
     * @return: A valid signature of the current transaction.
     */
    private byte[] computeSignature(final String txToComputeSignatureFor) {
        boolean isGrapheneCanonical = false;
        byte[] sigData = null;

        while (!isGrapheneCanonical) {
            byte[] serializedTransaction = DEFAULT_STRING_FOR_NEW_TRANSACTION.equals(txToComputeSignatureFor) ?
                    this.toBytes() : txToComputeSignatureFor.getBytes();
            Sha256Hash hash = Sha256Hash.wrap(Sha256Hash.hash(serializedTransaction));
            int recId = -1;
            ECKey.ECDSASignature sig = privateKey.sign(hash);

            // Now we have to work backwards to figure out the recId needed to recover the signature.
            for (int i = 0; i < 4; i++) {
                ECKey k = ECKey.recoverFromSignature(i, sig, hash, privateKey.isCompressed());
                if (k != null && k.getPubKeyPoint().equals(privateKey.getPubKeyPoint())) {
                    recId = i;
                    break;
                }
            }

            sigData = new byte[65];  // 1 header + 32 bytes for R + 32 bytes for S
            int headerByte = recId + 27 + (privateKey.isCompressed() ? 4 : 0);
            sigData[0] = (byte) headerByte;
            System.arraycopy(Utils.bigIntegerToBytes(sig.r, 32), 0, sigData, 1, 32);
            System.arraycopy(Utils.bigIntegerToBytes(sig.s, 32), 0, sigData, 33, 32);

            // Further "canonicality" tests
            if (((sigData[0] & 0x80) != 0) || (sigData[0] == 0) ||
                    ((sigData[1] & 0x80) != 0) || ((sigData[32] & 0x80) != 0) ||
                    (sigData[32] == 0) || ((sigData[33] & 0x80) != 0)) {
                this.blockData.setExpiration(this.blockData.getExpiration() + 1);
            } else {
                isGrapheneCanonical = true;
            }
        }
        return sigData;
    }

    /**
     * Method that creates a serialized byte array with compact information about this transaction
     * that is needed for the creation of a signature.
     *
     * @return: byte array with serialized information about this transaction.
     */
    @Suspendable
    public byte[] toBytes() {
        // Creating a List of Bytes and adding the first bytes from the chain apiId
        List<Byte> byteArray = new ArrayList<>();
        byteArray.addAll(Bytes.asList(chainId));

        // Adding the block data
        if (blockData != null) {
            byteArray.addAll(Bytes.asList(this.blockData.toBytes()));
        }

        // Adding the number of operations
        byteArray.add((byte) this.operations.size());

        // Adding all the operations
        for (final graphene.components.BaseOperation operation : operations) {
            byteArray.add(operation.getId());
            byteArray.addAll(Bytes.asList(operation.toBytes()));
        }

        // Adding extensions byte
        byteArray.addAll(Bytes.asList(this.extensions.toBytes()));

        return Bytes.toArray(byteArray);
    }

    public byte[] getChainId() {
        return this.chainId;
    }

    public void setChainId(final String chainId) {
        this.chainId = Util.hexToBytes(chainId);
    }

    public void setChainId(final byte[] chainId) {
        this.chainId = chainId;
    }

    @Suspendable
    @Override
    public String toJsonString() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Transaction.class, new TransactionSerializer());
        return gsonBuilder.create().toJson(this);
    }

    @Suspendable
    @Override
    public JsonObject toJsonObject() {
        JsonObject obj = new JsonObject();

        // Getting the signature before anything else,
        // since this might change the transaction expiration data slightly
        byte[] signature = Configuration.SIGN_LOCAL ? getGrapheneSignature() : this.signature;

        // Formatting expiration time
        if (blockData != null) {
            Date expirationTime = new Date(blockData.getExpiration() * 1000);
            SimpleDateFormat dateFormat = new SimpleDateFormat(Util.TIME_DATE_FORMAT);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

            // Adding expiration
            obj.addProperty(KEY_EXPIRATION, dateFormat.format(expirationTime));
        }

        JsonArray operationsArray = new JsonArray();
        for (final graphene.components.BaseOperation operation : operations) {
            operationsArray.add(operation.toJsonObject());
        }
        // Adding operations
        obj.add(KEY_OPERATIONS, operationsArray);

        // Adding extensions
        obj.add(KEY_EXTENSIONS, new JsonArray());

        // Adding block data
        if (blockData != null) {
            obj.addProperty(KEY_REF_BLOCK_NUM, blockData.getRefBlockNum());
            obj.addProperty(KEY_REF_BLOCK_PREFIX, blockData.getRefBlockPrefix());
        }

        // Adding signatures
        if (signature != null) {
            JsonArray signatureArray = new JsonArray();
            signatureArray.add(Util.bytesToHex(signature));
            obj.add(KEY_SIGNATURES, signatureArray);
        }

        return obj;

    }

    @Suspendable
    public byte[] getGrapheneSignature() {
        return computeSignature(DEFAULT_STRING_FOR_NEW_TRANSACTION);
    }

    @Suspendable
    public ImmutablePair<Boolean, String> getSignatureRemote(final Transaction transaction,
                                                             final String webSocketAddress) {

        /*List<String> servers = GenericSelectionStrategy.selectFixed(GrapheneHelper.getAccounts(true),
                Collections.singletonList(0), false);
        WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                servers.get(0));*/

        WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                webSocketAddress);

        SignTransaction signTransaction = new SignTransaction(transaction, true,
                new GrapheneWitnessListener().registerWitnessResponseListener());

        List<WebSocketListener> webSocketListenerList = new ArrayList<>();
        webSocketListenerList.add(signTransaction);

        GrapheneWebsocket.connectToServer(webSocket, webSocketListenerList);

        ImmutablePair<Boolean, String> signature;
        try {
            signature = signTransaction.getResponseFuture().get(Configuration.TIMEOUT_TRANSACTION,
                    Configuration.TIMEOUT_UNIT_TRANSACTION);
        } catch (InterruptedException | ExecutionException ex) {
            ExceptionHandler.logException(ex);
            return ImmutablePair.of(true, ex.getMessage());
        } catch (TimeoutException ex) {
            ExceptionHandler.logException(ex);
            return new ImmutablePair<>(true, "TIMEOUT_EX");
        }

        if (signature.getLeft()) {
            return signature;
        } else {

            JSONObject jsonObject = new JSONObject(Objects.requireNonNull(signature.getRight()));
            JSONObject result = jsonObject.getJSONObject("result");
            JSONArray signatures = result.getJSONArray("signatures");
            for (int i = 0; i < signatures.length(); i++) {
                LOG.info("Signature result: " + signatures.get(i));
            }
            LOG.info("Using signature: " + signatures.get(0) + " | no multisig");
            this.signature = Util.hexToBytes(String.valueOf(signatures.get(0)));
            return ImmutablePair.of(false, signature.getRight());
        }
    }

    @Suspendable
    public ImmutablePair<Boolean, String> getTxId(final Transaction transaction, final String webSocketAddress) {
        try {

                    /*List<String> servers = GenericSelectionStrategy.selectFixed(GrapheneHelper.getAccounts(true),
                Collections.singletonList(0), false);
        WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                servers.get(0));*/

            WebSocket webSocket = GrapheneWebsocket.prepareWebsocket(GrapheneWebsocket.prepareWebsocketFactory(),
                    webSocketAddress);

            TransactionId transactionId = new TransactionId(transaction, true,
                    new GrapheneWitnessListener().registerWitnessResponseListener());

            List<WebSocketListener> webSocketListenerList = new ArrayList<>();
            webSocketListenerList.add(transactionId);

            GrapheneWebsocket.connectToServer(webSocket, webSocketListenerList);

            ImmutablePair<Boolean, String> txId =
                    transactionId.getResponseFuture().get(Configuration.TIMEOUT_TRANSACTION,
                            Configuration.TIMEOUT_UNIT_TRANSACTION);
            LOG.info("TxId: " + txId);
            return txId;
        } catch (InterruptedException |
                ExecutionException ex) {
            ExceptionHandler.logException(ex);
            return ImmutablePair.of(true, ex.getMessage());
        } catch (TimeoutException ex) {
            ExceptionHandler.logException(ex);
            return new ImmutablePair<>(true, "TIMEOUT_EX");
        }
    }

    /**
     * Class used to encapsulate the procedure to be followed when converting a transaction from a
     * java object to its JSON string format representation.
     */
    public static class TransactionSerializer implements JsonSerializer<Transaction> {

        @Override
        public JsonElement serialize(final Transaction transaction, final Type type,
                                     final JsonSerializationContext jsonSerializationContext) {
            return transaction.toJsonObject();
        }
    }

    /**
     * Static inner class used to encapsulate the procedure to be followed when converting a transaction from its
     * JSON string format representation into a java object instance.
     */
    public static class TransactionDeserializer implements JsonDeserializer<Transaction> {

        @Override
        public Transaction deserialize(final JsonElement json, final Type typeOfT,
                                       final JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = json.getAsJsonObject();

            // Parsing block data information
            int refBlockNum = jsonObject.get(KEY_REF_BLOCK_NUM).getAsInt();
            long refBlockPrefix = jsonObject.get(KEY_REF_BLOCK_PREFIX).getAsLong();
            String expiration = jsonObject.get(KEY_EXPIRATION).getAsString();
            SimpleDateFormat dateFormat = new SimpleDateFormat(Util.TIME_DATE_FORMAT);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            Date expirationDate = dateFormat.parse(expiration, new ParsePosition(0));
            BlockData blockData = new BlockData(refBlockNum, refBlockPrefix, expirationDate.getTime());

            // Parsing operation list
            graphene.components.BaseOperation operation = null;
            ArrayList<BaseOperation> operationList = new ArrayList<>();

            for (final JsonElement jsonOperation : jsonObject.get(KEY_OPERATIONS).getAsJsonArray()) {
                int operationId = jsonOperation.getAsJsonArray().get(0).getAsInt();
                if (operationId == graphene.payloads.OperationType.TRANSFER_OPERATION.ordinal()) {
                    operation = context.deserialize(jsonOperation, TransferOperation.class);
                } else if (operationId == graphene.payloads.OperationType.LIMIT_ORDER_CREATE_OPERATION.ordinal()) {
                    operation = context.deserialize(jsonOperation, LimitOrderCreateOperation.class);
                } else if (operationId == graphene.payloads.OperationType.LIMIT_ORDER_CANCEL_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.CALL_ORDER_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.FILL_ORDER_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ACCOUNT_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ACCOUNT_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ACCOUNT_WHITELIST_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ACCOUNT_UPGRADE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ACCOUNT_TRANSFER_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_UPDATE_BITASSET_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_UPDATE_FEED_PRODUCERS_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_ISSUE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_RESERVE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_FUND_FEE_POOL_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_SETTLE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_GLOBAL_SETTLE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_PUBLISH_FEED_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WITNESS_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WITNESS_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.PROPOSAL_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.PROPOSAL_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.PROPOSAL_DELETE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WITHDRAW_PERMISSION_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WITHDRAW_PERMISSION_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WITHDRAW_PERMISSION_CLAIM_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WITHDRAW_PERMISSION_DELETE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.COMMITTEE_MEMBER_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.COMMITTEE_MEMBER_UPDATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.COMMITTEE_MEMBER_UPDATE_GLOBAL_PARAMETERS_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.VESTING_BALANCE_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.VESTING_BALANCE_WITHDRAW_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.WORKER_CREATE_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.CUSTOM_OPERATION.ordinal()) {
                    operation = context.deserialize(jsonOperation, CustomOperation.class);
                } else if (operationId == graphene.payloads.OperationType.ASSERT_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.BALANCE_CLAIM_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.OVERRIDE_TRANSFER_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.TRANSFER_TO_BLIND_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.BLIND_TRANSFER_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.TRANSFER_FROM_BLIND_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == graphene.payloads.OperationType.ASSET_SETTLE_CANCEL_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                } else if (operationId == OperationType.ASSET_CLAIM_FEES_OPERATION.ordinal()) {
                    //TODO: Add operation deserialization support
                }
                if (operation != null) operationList.add(operation);
                operation = null;
            }
            return new Transaction(blockData, operationList);
        }
    }
}