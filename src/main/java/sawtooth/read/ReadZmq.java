package sawtooth.read;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.model.DataItem;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.zeromq.ZMQ;
import sawtooth.configuration.Configuration;
import sawtooth.helper.SawtoothHelper;
import sawtooth.payloads.ISawtoothReadPayload;
import sawtooth.payloads.TpHandler;
import sawtooth.sdk.protobuf.ClientStateGetRequest;
import sawtooth.sdk.protobuf.ClientStateGetResponse;
import sawtooth.sdk.protobuf.Message;
import sawtooth.statistics.ReadStatisticObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ReadZmq implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(ReadZmq.class);

    private static final double TIMEOUT_VALUE = 1E9;

    private final CompletableFuture<ImmutablePair<Boolean, String>> done = new CompletableFuture<>();

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> read(final E... params) {
        if (params.length == 4) {

            ISawtoothReadPayload iSawtoothReadPayload = (ISawtoothReadPayload) params[0];
            ZMQ.Socket socket = (ZMQ.Socket) params[1];
            String correlationId = (String) params[2];
            ReadStatisticObject readStatisticObject = (ReadStatisticObject) params[3];

            return readDataPerZmq(iSawtoothReadPayload, socket, correlationId, readStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> readDataPerZmq(final ISawtoothReadPayload iSawtoothReadPayload, final ZMQ.Socket socket,
                                                          final String correlationId,
                                                          final ReadStatisticObject readStatisticObject) {

        readStatisticObject.setStartTime(System.nanoTime());

        String address = TpHandler.getTpRead(iSawtoothReadPayload.getTpEnum(), iSawtoothReadPayload.getTpPrefix(), iSawtoothReadPayload.getValueToRead());
        ClientStateGetRequest clientStateGetRequest = ClientStateGetRequest.newBuilder().setAddress(address).build();
        Message clientStateGetRequestMessage =
                Message.newBuilder().setCorrelationId(correlationId).setMessageType(Message.MessageType.CLIENT_STATE_GET_REQUEST).setContent(clientStateGetRequest.toByteString()).build();
        socket.send(clientStateGetRequestMessage.toByteArray());

        if (Configuration.RECEIVE_READ_REQUEST) {
            boolean received = false;
            long startTime = System.nanoTime();
            while (!received && !checkTimeout(startTime, readStatisticObject)) {
                byte[] recv = socket.recv(Configuration.ZMQ_SOCKET_FLAG_READ);
                if (recv != null) {
                    try {
                        Message from = Message.parseFrom(recv);
                        LOG.debug("Correlation ID: " + from.getCorrelationId() + " Message type: " + from.getMessageType() + " name: " + from.getMessageType().name());
                        if (from.getMessageType() == Message.MessageType.CLIENT_STATE_GET_RESPONSE) {
                            ClientStateGetResponse clientStateGetResponse =
                                    ClientStateGetResponse.parseFrom(from.getContent());
                            /*
                             * 1 - OK - everything worked as expected
                             * 2 - INTERNAL_ERROR - general error, such as protobuf failing to deserialize
                             * 3 - NOT_READY - the validator does not yet have a genesis block
                             * 4 - NO_ROOT - the state_root specified was not found
                             * 5 - NO_RESOURCE - the address specified doesn't exist
                             * 6 - INVALID_ADDRESS - address isn't a valid, i.e. it's a subtree (truncated)
                             */
                            LOG.info("1 = OK 2 = INTERNAL_ERROR 3 = NOT_READY 4 = NO_ROOT 5 = NO_RESOURCE 6 = " +
                                    "INVALID_ADDRESS 7 = INVALID_ROOT - Current state: " + clientStateGetResponse.getStatus().name());

                            if (clientStateGetResponse.getStatusValue() == ClientStateGetResponse.Status.OK_VALUE) {
                                if (Configuration.DECODE_DATA_AS_CBOR_READ) {
                                    try {

                                        ByteArrayInputStream byteArrayInputStream =
                                                new ByteArrayInputStream(clientStateGetResponse.getValue().toByteArray());
                                        List<DataItem> decodedReadValue =
                                                new CborDecoder(byteArrayInputStream).decode();
                                        byteArrayInputStream.close();
                                        //CborDecoder.decode(clientStateGetResponse.getValue().toByteArray());
                                        for (final DataItem dataItem : decodedReadValue) {
                                            LOG.info("Decoded data item : " + SawtoothHelper.decodeCbor(dataItem));

                                        }
                                        readStatisticObject.setEndTime(System.nanoTime());
                                        done.complete(ImmutablePair.of(false, ""));
                                    } catch (CborException ex) {
                                        LOG.error("Could not decode read request value");
                                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                        done.complete(ImmutablePair.of(true, ex.getMessage()));
                                    }
                                } else {
                                    readStatisticObject.setEndTime(System.nanoTime());
                                    done.complete(ImmutablePair.of(false, ""));
                                }
                                received = true;
                            } else {
                                LOG.error("Received wrong status: " + clientStateGetResponse.getStatus().name());
                                readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                done.complete(ImmutablePair.of(true,
                                        "Received wrong status: " + clientStateGetResponse.getStatus().name()));
                            }
                        }
                    } catch (IOException ex) {
                        ExceptionHandler.logException(ex);
                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        done.complete(ImmutablePair.of(true, ex.getMessage()));
                    }
                } else {
                    try {
                        Strand.sleep(Configuration.READ_ZMQ_SLEEP_TIME);
                    } catch (SuspendExecution | InterruptedException ex) {
                        ExceptionHandler.logException(ex);
                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        done.complete(ImmutablePair.of(true, ex.getMessage()));
                    }
                }
            }
        } else {
            readStatisticObject.setEndTime(-1);
            done.complete(ImmutablePair.of(false, "Sent async"));
        }
        try {
            return done.get(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);
        } catch (InterruptedException | ExecutionException ex) {
            ExceptionHandler.logException(ex);
            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        } catch (TimeoutException ex) {
            ExceptionHandler.logException(ex);
            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return new ImmutablePair<>(true, "TIMEOUT_EX");
        }
    }

    @Suspendable
    private boolean checkTimeout(final long startTime, final ReadStatisticObject readStatisticObject) {
        long currentTime = System.nanoTime();
        double convert = (currentTime - startTime) / TIMEOUT_VALUE;
        if (Configuration.ZMQ_READ_TIMEOUT <= convert) {
            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            done.complete(ImmutablePair.of(true,
                    "Timeout reached, start time: " + startTime + " current time: " + currentTime + " convert " + convert));
            return true;
        } else {
            LOG.trace("Convert: " + convert);
            return false;
        }
    }
}
