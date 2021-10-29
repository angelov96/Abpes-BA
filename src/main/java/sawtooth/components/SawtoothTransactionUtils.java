package sawtooth.components;

import co.paralleluniverse.fibers.Suspendable;
import com.google.protobuf.ByteString;
import sawtooth.payloads.ISawtoothWritePayload;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.protobuf.TransactionHeader;
import sawtooth.sdk.protobuf.TransactionList;
import sawtooth.sdk.signing.Signer;

import java.io.ByteArrayOutputStream;
import java.util.List;

public class SawtoothTransactionUtils {

    private SawtoothTransactionUtils() {
    }

    @Suspendable
    public static ByteString prepareTransactionListForForeignSender(final Transaction... transactions) {
        // Only necessary when another machine is sending the transaction(s)
        TransactionList.Builder transactionList = TransactionList.newBuilder();
        for (final Transaction transaction : transactions) {
            transactionList.addTransactions(transaction);
        }

        return transactionList.build().toByteString();
    }

    @Suspendable
    public static Transaction addTransactionToList(final Signer signer, final String familyName,
                                                   final String familyVersion,
                                                   final List<Transaction> transactionList,
                                                   final ISawtoothWritePayload payload) {
        ByteArrayOutputStream byteArrayOutputStream = payload.getPayload();
        byte[] payloadByteArray = byteArrayOutputStream.toByteArray();

        List<String> inputAddresses = payload.getInputAddresses(payload.getPlainAddressValues());
        List<String> outputAddresses = payload.getOutputAddresses(payload.getPlainAddressValues());

        TransactionHeader transactionHeader = SawtoothTransactionHeaderUtils.buildTransactionHeader(signer,
                byteArrayOutputStream,
                inputAddresses, outputAddresses, familyName, familyVersion);

        String signature = signer.sign(transactionHeader.toByteArray());

        Transaction transaction = SawtoothTransactionUtils.buildTransaction(transactionHeader, payloadByteArray,
                signature);
        transactionList.add(transaction);
        return transaction;
    }

    @Suspendable
    public static Transaction buildTransaction(final TransactionHeader transactionHeader,
                                               final byte[] payloadByteArray, final String signature) {
        return Transaction.newBuilder()
                .setHeader(transactionHeader.toByteString())
                .setPayload(ByteString.copyFrom(payloadByteArray))
                .setHeaderSignature(signature)
                .build();
    }

}
