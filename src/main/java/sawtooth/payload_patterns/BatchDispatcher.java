package sawtooth.payload_patterns;

import client.client.ClientObject;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import sawtooth.components.SawtoothBatchUtils;
import sawtooth.components.SawtoothTransactionUtils;
import sawtooth.configuration.Configuration;
import sawtooth.listener.ZmqListener;
import sawtooth.payloads.ISawtoothWritePayload;
import sawtooth.sdk.protobuf.Batch;
import sawtooth.sdk.protobuf.Transaction;
import sawtooth.sdk.signing.Signer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchDispatcher implements ITransactionToBatchDispatcher {

    private static final Logger LOG = Logger.getLogger(BatchDispatcher.class);

    private final Map<Transaction, ISawtoothWritePayload> transactionMapping = new HashMap<>();

    public Map<Transaction, ISawtoothWritePayload> getPayloadMapping() {
        return transactionMapping;
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E dispatchTransactions(final E... params) {

        if (params.length != 5) {
            throw new NotYetImplementedException("Not yet implemented");
        }
        List<ISawtoothWritePayload> payloads = (List<ISawtoothWritePayload>) params[0];
        Signer signer = (Signer) params[1];
        int batchTransactionThreshold = (Integer) params[2];
        List<Batch> batchListToSend = (List<Batch>) params[3];
        ClientObject clientObject = (ClientObject) params[4];

        List<Batch> batches = new ArrayList<>(batchListToSend);

        List<Transaction> transactionList = new ArrayList<>();
        int transactionsInBatch = 0;

        LOG.info("Dispatching a total of: " + payloads.size() + " payloads to " + Configuration.NUMBER_OF_BATCHES_PER_CLIENT
                + " batches, split into " + Configuration.NUMBER_OF_TRANSACTIONS_PER_BATCH_PER_CLIENT +
                " transactions");

        for (final ISawtoothWritePayload payload : payloads) {

            transactionMapping.put(
                    SawtoothTransactionUtils.addTransactionToList(signer, payload.getFamilyName(),
                            payload.getFamilyVersion(), transactionList, payload),
                    payload
            );

            transactionsInBatch++;

            ZmqListener.getObtainedEventsMap().get(clientObject.getClientId()).get(payload.getEventPrefix() + payload.getSignature()).setLeft(System.nanoTime());

            if (transactionsInBatch >= batchTransactionThreshold) {
                LOG.info("Reached threshold of transactions, splitting batch");
                batches.add(SawtoothBatchUtils.prepareBatch(transactionList, signer));
                return (E) SawtoothBatchUtils.buildBatchList(batches);
            }

        }

        batches.add(SawtoothBatchUtils.prepareBatch(transactionList, signer));
        return (E) SawtoothBatchUtils.buildBatchList(batches);

    }

}