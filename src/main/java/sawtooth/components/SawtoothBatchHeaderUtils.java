package sawtooth.components;

import co.paralleluniverse.fibers.Suspendable;
import sawtooth.sdk.protobuf.BatchHeader;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.signing.Signer;

import java.util.List;
import java.util.stream.Collectors;

public class SawtoothBatchHeaderUtils {

    private SawtoothBatchHeaderUtils() {
    }

    @Suspendable
    public static BatchHeader buildBatchHeader(final Signer signer, final List<Transaction> transactions) {
        return BatchHeader.newBuilder()
                .setSignerPublicKey(signer.getPublicKey().hex())
                .addAllTransactionIds(
                        transactions
                                .stream()
                                .map(Transaction::getHeaderSignature)
                                .collect(Collectors.toList())
                )
                .build();
    }

}
