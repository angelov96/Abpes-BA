package sawtooth.write;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import co.paralleluniverse.strands.StrandLocalRandom;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.zeromq.ZMQ;
import sawtooth.configuration.Configuration;
import sawtooth.sdk.protobuf.*;
import sawtooth.statistics.WriteStatisticObject;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WriteZmq implements IWritingMethod {

    private static final Logger LOG = Logger.getLogger(WriteZmq.class);
    private final double threshold;
    private final long rePollInterval;
    private int currentRePolls = 0;
    private double committedBatches = 0;

    public CompletableFuture<Boolean> getIsDone() {
        return isDone;
    }

    private CompletableFuture<Boolean> isDone = new CompletableFuture<>();

    public WriteZmq(final double thresholdConstructor, final long rePollIntervalConstructor) {
        threshold = thresholdConstructor;
        rePollInterval = rePollIntervalConstructor;
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> write(final E... params) {
        if (params.length == 4) {

            BatchList batchListBytes = (BatchList) params[0];
            String correlationId = (String) params[1];
            ZMQ.Socket socket = (ZMQ.Socket) params[2];
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[3];

            return sendBatchesPerZmq(batchListBytes, correlationId, socket, writeStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> sendBatchesPerZmq(final BatchList batchListBytes, final String correlationId,
                                                             final ZMQ.Socket socket,
                                                             final WriteStatisticObject writeStatisticObject) {

        writeStatisticObject.setStartTime(System.nanoTime());

        try {

            Message clientBatchSubmitRequest =
                    Message.newBuilder().setCorrelationId(correlationId).setMessageType(Message.MessageType.CLIENT_BATCH_SUBMIT_REQUEST).setContent(ByteString.copyFrom(batchListBytes.toByteArray())).build();
            boolean send = socket.send(clientBatchSubmitRequest.toByteArray());
            if (!send) {
                throw new Exception("Send process not successful exception");
            }

            if (Configuration.SEND_WRITE_ASYNC) {
                writeStatisticObject.setEndTime(-1);
                isDone.complete(true);
                LOG.debug("Sent async");
                return ImmutablePair.of(false, "");
            } else {
                Message clientBatchStatusRequestMessage = null;
                if (Configuration.CHECK_BATCH_STATUS) {
                    ClientBatchStatusRequest.Builder clientBatchStatusRequestBuilder =
                            ClientBatchStatusRequest.newBuilder();
                    for (final Batch batch : batchListBytes.getBatchesList()) {
                        clientBatchStatusRequestBuilder.addBatchIdsBytes(batch.getHeaderSignatureBytes());
                    }

                    LOG.info("Batch list size: " + batchListBytes.getBatchesList().size());
                    ClientBatchStatusRequest clientBatchStatusRequest = clientBatchStatusRequestBuilder.build();
                    clientBatchStatusRequestMessage =
                            Message.newBuilder().setCorrelationId(correlationId).setMessageType(Message.MessageType.CLIENT_BATCH_STATUS_REQUEST).setContent(clientBatchStatusRequest.toByteString()).build();
                    boolean send1 = socket.send(clientBatchStatusRequestMessage.toByteArray());
                    if (!send1) {
                        throw new Exception("Send process not successful exception");
                    }
                }

                long start = System.currentTimeMillis();
                long end = start + TimeUnit.MILLISECONDS.convert(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);
                while (true) {
                    byte[] recv = socket.recv(Configuration.ZMQ_SOCKET_FLAG_WRITE);
                    if (recv != null) {
                        try {
                            Message from = Message.parseFrom(recv);
                            LOG.debug("Correlation ID: " + from.getCorrelationId() + " Message type: " + from.getMessageType() + " name: " + from.getMessageType().name());

                            if (from.getMessageType() == Message.MessageType.CLIENT_BATCH_SUBMIT_RESPONSE) {

                                ClientBatchSubmitResponse clientBatchSubmitResponse =
                                        ClientBatchSubmitResponse.parseFrom(from.getContent());
                                /*
                                 * 1  OK - everything with the request worked as expected
                                 * 2  INTERNAL_ERROR - general error, such as protobuf failing to deserialize
                                 * 3  INVALID_BATCH - the batch failed validation, likely due to a bad signature
                                 * 4  QUEUE_FULL - the batch is unable to be queued for processing, due to
                                 *    a full processing queue.  The batch may be submitted again.
                                 * -1 unrecognized
                                 */
                                LOG.info("1 = OK 2 = INTERNAL_ERROR 3 = INVALID_BATCH 4 = QUEUE_FULL -1 = " +
                                        "UNRECOGNIZED: Current status: " + clientBatchSubmitResponse.getStatus());

                                if (clientBatchSubmitResponse.getStatus() == ClientBatchSubmitResponse.Status.QUEUE_FULL) {
                                    LOG.error("Exception Queue full");
                                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                    try {
                                        int rt = StrandLocalRandom.current().nextInt(
                                                Integer.parseInt(Configuration.QUEUE_FULL_RESUBMIT_INTERVAL.split("-")[0]),
                                                Integer.parseInt(Configuration.QUEUE_FULL_RESUBMIT_INTERVAL.split("-")[1]) + 1);
                                        Strand.sleep(rt);
                                        LOG.debug("Sleeping queue full interval: " + rt);
                                    } catch (SuspendExecution | InterruptedException ex) {
                                        ExceptionHandler.logException(ex);
                                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                        isDone.complete(true);
                                        return ImmutablePair.of(true, ex.getMessage());
                                    }
                                    isDone.complete(true);
                                    return ImmutablePair.of(true, clientBatchSubmitResponse.getStatus().name());
                                }

                                if (clientBatchSubmitResponse.getStatus() != ClientBatchSubmitResponse.Status.OK) {
                                    LOG.error("Invalid status " + clientBatchSubmitResponse.getStatus().name());
                                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                    isDone.complete(true);
                                    return ImmutablePair.of(true, clientBatchSubmitResponse.getStatus().name());
                                }

                                if (!Configuration.CHECK_BATCH_STATUS) {
                                    writeStatisticObject.setEndTime(System.nanoTime());
                                    isDone.complete(true);
                                    return ImmutablePair.of(false, from.getCorrelationId());
                                }

                            } else if (from.getMessageType() == Message.MessageType.CLIENT_BATCH_STATUS_RESPONSE) {
                                ClientBatchStatusResponse clientBatchStatusResponse =
                                        ClientBatchStatusResponse.parseFrom(from.getContent());

                                LOG.info("Batches count: " + clientBatchStatusResponse.getBatchStatusesCount());

                                committedBatches = 0;
                                for (final ClientBatchStatus clientBatchStatus :
                                        clientBatchStatusResponse.getBatchStatusesList()) {

                                    List<ClientBatchStatus.InvalidTransaction> invalidTransactions =
                                            clientBatchStatus.getInvalidTransactionsList();
                                    LOG.info("Invalid transaction list size: " + invalidTransactions.size());
                                    /*
                                     * Statuses:
                                     * 1    COMMITTED - the batch was accepted and has been committed to the chain
                                     * 2    INVALID - the batch failed validation, it should be resubmitted
                                     * 3    PENDING - the batch is still being processed
                                     * 4    UNKNOWN - no status for the batch could be found (possibly invalid)
                                     */
                                    LOG.info("1 = COMMITTED 2 = INVALID 3 = PENDING 4 = UNKNOWN - current status: " + clientBatchStatus.getStatus().name());
                                    if (clientBatchStatus.getStatusValue() == ClientBatchStatusResponse.Status.OK_VALUE) {
                                        ++committedBatches;
                                        if (checkThreshold(clientBatchStatusResponse)) {
                                            writeStatisticObject.setEndTime(System.nanoTime());
                                            isDone.complete(true);
                                            return ImmutablePair.of(false,
                                                    clientBatchStatusResponse.getStatus().name());
                                        }
                                    } else if (clientBatchStatus.getStatusValue() == ClientBatchStatusResponse.Status.INVALID_ID_VALUE ||
                                            clientBatchStatus.getStatusValue() == ClientBatchStatusResponse.Status.NO_RESOURCE_VALUE ||
                                            clientBatchStatus.getStatusValue() == ClientBatchStatusResponse.Status.INTERNAL_ERROR_VALUE) {
                                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                        isDone.complete(true);
                                        return ImmutablePair.of(true,
                                                "Got invalid value for: " + clientBatchStatus.getBatchId() + " status" +
                                                        ":" +
                                                        " " + clientBatchStatus.getStatus() +
                                                        " aborting");
                                    } else {
                                        if (currentRePolls < Configuration.CHECK_COMMITTED_REPOLLS) {
                                            LOG.info("Recalling for: " + clientBatchStatus.getBatchId() + " status: " + clientBatchStatus.getStatus() +
                                                    " repoll number: " + currentRePolls + ", waiting: " + rePollInterval);
                                            ++currentRePolls;
                                            try {
                                                Strand.sleep(rePollInterval);
                                            } catch (SuspendExecution | InterruptedException ex) {
                                                ExceptionHandler.logException(ex);
                                                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                                isDone.complete(true);
                                                return ImmutablePair.of(true, ex.getMessage());
                                            }
                                            boolean send1 =
                                                    socket.send(Objects.requireNonNull(clientBatchStatusRequestMessage).toByteArray());
                                            if (!send1) {
                                                throw new Exception("Send process not successful exception");
                                            }
                                        } else {
                                            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                            isDone.complete(true);
                                            return ImmutablePair.of(true,
                                                    "Too many repolls for: " + clientBatchStatus.getBatchId() + " " +
                                                            "status:" +
                                                            " " + clientBatchStatus.getStatus() +
                                                            " aborting");
                                        }
                                    }
                                }
                            } else if (from.getMessageType() == Message.MessageType.PING_REQUEST) {
                                LOG.trace("Received ping request");
                                if (Configuration.SEND_PING_RESPONSES) {
                                    Message message =
                                            Message.newBuilder().setMessageType(Message.MessageType.PING_RESPONSE).setContent(NetworkAcknowledgement.newBuilder().setStatus(NetworkAcknowledgement.Status.OK).build().toByteString()).build();
                                    boolean successfullySent = socket.send(message.toByteArray());
                                    LOG.trace("Successfully sent ping response: " + successfullySent + " from" +
                                            " " + Strand.currentStrand().getName());
                                }
                            } else {
                                LOG.error("Received unexpected message, not aborting Type:" + from.getMessageType());
                            }
                        } catch (InvalidProtocolBufferException ex) {
                            ExceptionHandler.logException(ex);
                            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            isDone.complete(true);
                            return ImmutablePair.of(true, ex.getMessage());
                        }
                    } else {
                        try {
                            Strand.sleep(Configuration.WRITE_ZMQ_SLEEP_TIME);
                        } catch (SuspendExecution | InterruptedException ex) {
                            ExceptionHandler.logException(ex);
                            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            isDone.complete(true);
                            return ImmutablePair.of(true, ex.getMessage());
                        }
                    }
                    if(System.currentTimeMillis() > end) {
                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        LOG.error("Timeout reached during write loop");
                        isDone.complete(true);
                        return new ImmutablePair<>(true, "TIMEOUT_EX");
                    }
                }
            }
        } catch (/*todo specify concrete exception(s)*/ Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            isDone.complete(true);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

    @Suspendable
    private boolean checkThreshold(final ClientBatchStatusResponse clientBatchStatusResponse) {
        if (threshold <= (committedBatches / clientBatchStatusResponse.getBatchStatusesCount())) {
            LOG.info("Reached threshold of " + threshold + " aborting with value: " + committedBatches / clientBatchStatusResponse.getBatchStatusesCount() + " committed batches: " + committedBatches + " number of batches " + clientBatchStatusResponse.getBatchStatusesCount());
            return true;
        }
        return false;
    }

}
