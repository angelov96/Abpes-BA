package sawtooth.write;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import retrofit2.Call;
import sawtooth.configuration.Configuration;
import sawtooth.endpointdata.batches_post.Batch;
import sawtooth.rest.ISawtoothClientRest;
import sawtooth.rest.RetrofitHelper;
import sawtooth.statistics.WriteStatisticObject;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class WriteWebsocket implements IWritingMethod {

    private static final Logger LOG = Logger.getLogger(WriteWebsocket.class);
    private static final String MEDIA_TYPE = "application/octet-stream";

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> write(final E... params) {
        if (params.length == 5) {

            byte[] batchListBytes = (byte[]) params[0];
            String url = (String) params[1];
            double threshold = Double.parseDouble(String.valueOf(params[2]));
            long rePollInterval = Long.parseLong(String.valueOf(params[3]));
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[4];

            return sendBatchesPerWebsocket(batchListBytes, url, threshold, rePollInterval, writeStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> sendBatchesPerWebsocket(final byte[] batchListBytes, final String url,
                                                                   final double threshold,
                                                                   final long rePollInterval,
                                                                   final WriteStatisticObject writeStatisticObject) {

        writeStatisticObject.setStartTime(System.nanoTime());

        try {

            retrofit2.Retrofit retrofit = RetrofitHelper.buildRetrofit(url);
            ISawtoothClientRest sawtoothClientRestInterface = retrofit.create(ISawtoothClientRest.class);

            RequestBody requestBody = RequestBody.create(
                    batchListBytes, MediaType.parse(MEDIA_TYPE));

            Call<Batch> call = sawtoothClientRestInterface.postBatchList(requestBody);

            TransactionWebsocketCallback sawtoothWebsocket =
                    new TransactionWebsocketCallback(sawtoothClientRestInterface
                    , threshold, rePollInterval, writeStatisticObject);

            call.enqueue(sawtoothWebsocket);

            try {
                return sawtoothWebsocket.getResponseFuture().get(Configuration.TIMEOUT_TRANSACTION,
                        Configuration.TIMEOUT_UNIT_TRANSACTION);
            } catch (InterruptedException | ExecutionException ex) {
                ExceptionHandler.logException(ex);
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return ImmutablePair.of(true, ex.getMessage());
            } catch (TimeoutException ex) {
                ExceptionHandler.logException(ex);
                writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                return new ImmutablePair<>(true, "TIMEOUT_EX");
            }

        } catch (/*todo specify concrete exception(s)*/ Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

}
