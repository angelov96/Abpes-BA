package sawtooth.write;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import sawtooth.configuration.Configuration;
import sawtooth.endpointdata.batch_status.BatchStatus;
import sawtooth.endpointdata.batch_status.Datum;
import sawtooth.endpointdata.batches_post.Batch;
import sawtooth.rest.ISawtoothClientRest;
import sawtooth.sdk.protobuf.ClientBatchStatus;
import sawtooth.statistics.WriteStatisticObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class TransactionWebsocketCallback implements Callback<Batch> {

    private static final Logger LOG = Logger.getLogger(TransactionWebsocketCallback.class);

    private final ISawtoothClientRest sawtoothClientRestInterface;

    private final CompletableFuture<ImmutablePair<Boolean, String>> responseFuture = new CompletableFuture<>();

    private final double threshold;
    private final long rePollInterval;
    private final WriteStatisticObject writeStatisticObject;
    private int currentRePolls = 0;
    private double committedBatches = 0;

    public TransactionWebsocketCallback(final ISawtoothClientRest sawtoothClientRestInterfaceConstructor,
                                        final double thresholdConstructor, final long rePollIntervalConstructor,
                                        final WriteStatisticObject writeStatisticObjectConstructor) {
        this.sawtoothClientRestInterface = sawtoothClientRestInterfaceConstructor;
        this.threshold = thresholdConstructor;
        this.rePollInterval = rePollIntervalConstructor;
        this.writeStatisticObject = writeStatisticObjectConstructor;

    }

    @Suspendable
    public CompletableFuture<ImmutablePair<Boolean, String>> getResponseFuture() {
        return responseFuture;
    }

    @Suspendable
    @Override
    public void onResponse(@NotNull final Call<Batch> call, @NotNull final Response<Batch> response) {
        if (Configuration.SEND_WRITE_ASYNC) {
            writeStatisticObject.setEndTime(System.nanoTime());
            responseFuture.complete(ImmutablePair.of(false, response.message()));
        } else {
            if (response.body() != null) {
                if (response.code() != 202) {
                    LOG.error("Unexpected response code: " + response.code() + " not 202");
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    responseFuture.complete(ImmutablePair.of(true, response.message()));
                    return;
                }

                LOG.info("Link: " + response.body().getLink());

                if (!Configuration.CHECK_BATCH_STATUS) {
                    writeStatisticObject.setEndTime(System.nanoTime());
                    responseFuture.complete(ImmutablePair.of(false, response.message()));
                    return;
                }

                String batchIdLink = response.body().getLink().split("=")[1];
                String[] batchIdsFromLink = batchIdLink.split(",");

                if (Configuration.USE_POST_FOR_BATCH_CHECK) {
                    List<String> batchIdsFromList = new ArrayList<>();
                    for (final String batchIdFromLink : batchIdsFromLink) {
                        batchIdsFromList.add("\"" + batchIdFromLink + "\"");
                    }
                    checkTransactionStatus(batchIdsFromList);
                } else {
                    List<String> batchIds = new ArrayList<>(Arrays.asList(batchIdsFromLink));

                    Call<BatchStatus> batchStatus =
                            sawtoothClientRestInterface.getBatchStatus(batchIds,
                                    Configuration.TIMEOUT_BATCH_STATUS);
                    batchStatus.enqueue(pollForResult());
                }
            } else {
                try {
                    LOG.error("Error: " + response.message() + " Code: " + response.code() + " ErrorBody: " + Objects.requireNonNull(response.errorBody()).string());
                } catch (IOException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    responseFuture.complete(ImmutablePair.of(true, ex.getMessage()));
                    return;
                }
                LOG.error("Response body was null");
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                responseFuture.complete(ImmutablePair.of(true, response.message()));
            }
        }
    }

    @Suspendable
    @Override
    public void onFailure(@NotNull final Call<Batch> call, final Throwable throwable) {
        LOG.error("Failure: " + throwable.getMessage());
        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
        responseFuture.complete(ImmutablePair.of(true, throwable.getMessage()));
    }

    @Suspendable
    private void checkTransactionStatus(final List<String> batchIdsFromList) {
        String MEDIA_TYPE = "application/json";

        RequestBody requestBody = RequestBody.create(
                batchIdsFromList.toString().getBytes(StandardCharsets.UTF_8), MediaType.parse(MEDIA_TYPE));

        Call<BatchStatus> batchStatus =
                sawtoothClientRestInterface.getBatchStatus(requestBody,
                        Configuration.TIMEOUT_BATCH_STATUS);

        batchStatus.enqueue(pollForResult());
    }

    @Suspendable
    private Callback<BatchStatus> pollForResult() {
        return new Callback<BatchStatus>() {
            private int writeRepollRetries;

            @Override
            public void onResponse(@NotNull final Call<BatchStatus> call,
                                   @NotNull final Response<BatchStatus> response) {
                if (response.body() != null) {
                    if (response.code() != 200) {
                        LOG.error("Unexpected response code: " + response.code() + " not 200");
                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        responseFuture.complete(ImmutablePair.of(true, response.message()));
                        return;
                    }
                    LOG.info("Data size: " + Objects.requireNonNull(response.body()).getData().size());

                    for (final Datum datum : (Objects.requireNonNull(response.body())).getData()) {
                        // ‘COMMITTED’, ‘INVALID’, ‘PENDING’, and ‘UNKNOWN’
                        if ("COMMITTED".equals(Objects.requireNonNull(datum.getStatus()))) {
                            LOG.info("Received committed batch number: " + (committedBatches + 1));
                            ++committedBatches;
                        }
                        else if("INVALID".equals(Objects.requireNonNull(datum.getStatus())) ||
                                "UNKNOWN".equals(Objects.requireNonNull(datum.getStatus()))) {
                            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            responseFuture.complete(ImmutablePair.of(true,
                                    "Invalid value for: " + datum.getId() + " status: " + datum.getStatus() +
                                            " aborting"));
                            return;
                        }
                        else {
                            if (currentRePolls < Configuration.CHECK_COMMITTED_REPOLLS) {
                                LOG.info("Recalling for: " + datum.getId() + " status: " + datum.getStatus() +
                                        " repoll number: " + currentRePolls + ", waiting: " + rePollInterval);
                                ++currentRePolls;
                                try {
                                    Strand.sleep(rePollInterval);
                                } catch (SuspendExecution | InterruptedException ex) {
                                    ExceptionHandler.logException(ex);
                                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                    responseFuture.complete(ImmutablePair.of(true, ex.getMessage()));
                                    return;
                                }
                                call.clone().enqueue(pollForResult());
                            } else {
                                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                                responseFuture.complete(ImmutablePair.of(true,
                                        "Too many repolls for: " + datum.getId() + " status: " + datum.getStatus() +
                                                " aborting"));
                                return;
                            }
                        }
                        List<ClientBatchStatus.InvalidTransaction> invalidTransactions =
                                (List<ClientBatchStatus.InvalidTransaction>) (List<?>) datum.getInvalidTransactions();
                        LOG.info("Invalid transactions: " + invalidTransactions.size());

                        if (checkThreshold(response)) {
                            writeStatisticObject.setEndTime(System.nanoTime());
                            responseFuture.complete(ImmutablePair.of(false, response.message()));
                            return;
                        }

                    }
                    try {
                        Strand.sleep(Configuration.WAIT_TIME_AFTER_CHECK_WRITE_REQUEST);
                    } catch (SuspendExecution | InterruptedException ex) {
                        ExceptionHandler.logException(ex);
                    } finally {
                        if (Configuration.RETRY_WRITE_POLL_ON_FAIL && writeRepollRetries <= Configuration.WRITE_POLL_RETRIES) {
                            writeRepollRetries++;
                            //call.enqueue(this);
                            call.clone().enqueue(pollForResult());
                        } else {
                            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            responseFuture.complete(ImmutablePair.of(true,
                                    "Did not complete successfully, number of retries: " + writeRepollRetries + " " +
                                            "Error " +
                                            "message: " + Objects.requireNonNull(response.toString())));
                        }
                    }
                } else {
                    try {
                        LOG.error("Error: " + response.message() + " Code: " + response.code() + " ErrorBody: " + Objects.requireNonNull(response.errorBody()).string());
                    } catch (IOException ex) {
                        ExceptionHandler.logException(ex);
                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        responseFuture.complete(ImmutablePair.of(true, ex.getMessage()));
                        return;
                    }
                    LOG.error("Response body was null");
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    responseFuture.complete(ImmutablePair.of(true, response.message()));
                }
            }

            @Override
            public void onFailure(@NotNull final Call<BatchStatus> call, @NotNull final Throwable throwable) {
                LOG.error("Failure: " + throwable.getMessage() + ", number of retries: " + writeRepollRetries);
                if (Configuration.RETRY_WRITE_POLL_ON_FAIL && writeRepollRetries <= Configuration.WRITE_POLL_RETRIES) {
                    writeRepollRetries++;
                    //call.enqueue(this);
                    call.clone().enqueue(pollForResult());
                } else {
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    responseFuture.complete(ImmutablePair.of(true, throwable.getMessage()));
                }
            }

        };
    }

    @Suspendable
    private boolean checkThreshold(final @NotNull Response<BatchStatus> response) {
        if (threshold <= (committedBatches / Objects.requireNonNull(response.body()).getData().size())) {
            LOG.info("Reached threshold of " + threshold + " aborting with value: " + committedBatches / response.body().getData().size() + " committed batches: " + committedBatches + " number of batches " + response.body().getData().size());
            return true;
        }
        return false;
    }
}
