package graphene.read;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import graphene.configuration.Configuration;
import graphene.statistics.ReadStatisticObject;
import okhttp3.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.io.IOException;
import java.util.Objects;

public class Read implements IReadingMethod {

    private static final MediaType JSON
            = MediaType.parse("application/json; charset=utf-8");
    private static final Logger LOG = Logger.getLogger(Read.class);

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> E read(final E... params) {
        if (params.length == 3) {

            String jsonRpcString = (String) params[0];
            String address = (String) params[1];
            ReadStatisticObject readStatisticObject = (ReadStatisticObject) params[2];

            return (E) read(jsonRpcString, address, readStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> read(final String jsonRpcString, final String address,
                                                final ReadStatisticObject readStatisticObject) {

        readStatisticObject.setStartTime(System.nanoTime());

        RequestBody body = RequestBody.create(jsonRpcString, JSON);

        OkHttpClient client = new OkHttpClient.Builder()
                .build();

        Request request = new Request.Builder()
                .url(address)
                .post(body)
                .build();

        Call call = client.newCall(request);

        try {
            Response response = call.execute();
            if (Configuration.RECEIVE_READ_REQUEST) {
                String responseBody = Objects.requireNonNull(response.body()).string();

                for (final String errorMessage : Configuration.ERROR_MESSAGES) {
                    if (responseBody.contains(errorMessage)) {
                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        return ImmutablePair.of(true, responseBody);
                    }
                }

                LOG.info("Message: " + response.message() + " Body: " + responseBody);
                readStatisticObject.setEndTime(System.nanoTime());
                return ImmutablePair.of(false, responseBody);
            } else {
                readStatisticObject.setEndTime(-1);
                return ImmutablePair.of(false, "");
            }
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

}
