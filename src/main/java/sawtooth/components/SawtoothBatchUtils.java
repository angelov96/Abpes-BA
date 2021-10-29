package sawtooth.components;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;
import sawtooth.configuration.Configuration;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.BatchHeader;
import sawtooth.sdk.protobuf.BatchList;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.signing.Signer;

import java.util.List;

public class SawtoothBatchUtils {

    private static final Logger LOG = Logger.getLogger(SawtoothBatchUtils.class);

    private SawtoothBatchUtils() {
    }

    @Suspendable
    public static BatchList buildBatchList(final List<Batch> batches) {
        BatchList.Builder batchListBuilder = BatchList.newBuilder();

        batches.forEach(batchListBuilder::addBatches);

        return batchListBuilder.build();
    }

    @Suspendable
    public static Batch prepareBatch(final List<Transaction> transactionList, final Signer signer) {
        BatchHeader batchHeader = SawtoothBatchHeaderUtils.buildBatchHeader(signer, transactionList);
        String batchSignature = signer.sign(batchHeader.toByteArray());

        boolean enableTrace = Configuration.ENABLE_TRACE;
        return SawtoothBatchUtils.buildBatch(batchHeader, transactionList, batchSignature, enableTrace);
    }

    @Suspendable
    public static Batch buildBatch(final BatchHeader batchHeader, final List<Transaction> transactions,
                                   final String batchSignature, final boolean enableTrace) {
        Batch.Builder batchBuilder = Batch.newBuilder()
                .setTrace(enableTrace)
                .setHeader(batchHeader.toByteString())
                .setHeaderSignature(batchSignature);
        transactions.forEach(batchBuilder::addTransactions);

        transactions.forEach(txId -> LOG.info("TxId: " + txId.getHeaderSignature() + " added to batch " + batchBuilder.getHeaderSignature()));
        return batchBuilder.build();
    }

}
