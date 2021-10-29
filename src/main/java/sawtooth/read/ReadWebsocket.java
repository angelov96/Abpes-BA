package sawtooth.read;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.model.DataItem;
import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.jetbrains.annotations.NotNull;
import org.spongycastle.util.encoders.Base64;
import org.spongycastle.util.encoders.DecoderException;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import sawtooth.configuration.Configuration;
import sawtooth.endpointdata.singlestate.SingleState;
import sawtooth.payloads.ISawtoothReadPayload;
import sawtooth.payloads.TpHandler;
import sawtooth.rest.ISawtoothClientRest;
import sawtooth.rest.RetrofitHelper;
import sawtooth.statistics.ReadStatisticObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ReadWebsocket implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(ReadWebsocket.class);

    private final CompletableFuture<ImmutablePair<Boolean, String>> done = new CompletableFuture<>();

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> read(final E... params) {
        if (params.length == 3) {
            String validatorAddress = (String) params[0];
            ISawtoothReadPayload iSawtoothReadPayload = (ISawtoothReadPayload) params[1];
            ReadStatisticObject readStatisticObject = (ReadStatisticObject) params[2];

            return readDataPerWebsocket(validatorAddress, iSawtoothReadPayload, readStatisticObject);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> readDataPerWebsocket(final String validatorAddress,
                                                                final ISawtoothReadPayload iSawtoothReadPayload,
                                                                final ReadStatisticObject readStatisticObject) {

        readStatisticObject.setStartTime(System.nanoTime());

        retrofit2.Retrofit retrofit = RetrofitHelper.buildRetrofit(validatorAddress);
        ISawtoothClientRest sawtoothClientRestInterface = retrofit.create(ISawtoothClientRest.class);

        String address = TpHandler.getTpRead(iSawtoothReadPayload.getTpEnum(), iSawtoothReadPayload.getTpPrefix(), iSawtoothReadPayload.getValueToRead());
        Call<SingleState> state = sawtoothClientRestInterface.getState(
                address, null);

        state.enqueue(new Callback<SingleState>() {
            @Suspendable
            @Override
            public void onResponse(@NotNull final Call<SingleState> call,
                                   @NotNull final Response<SingleState> response) {
                if (Configuration.RECEIVE_READ_REQUEST) {
                    if (response.body() != null) {
                        if (response.code() != 200) {
                            LOG.error("Unexpected response code: " + response.code() + " not 200");
                            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            done.complete(ImmutablePair.of(true, response.message()));
                            return;
                        }
                        try {

                            if (Configuration.DECODE_DATA_AS_CBOR_READ) {

                                ByteArrayInputStream byteArrayInputStream =
                                        new ByteArrayInputStream(Base64.decode(response.body().getData()));
                                List<DataItem> decode =
                                        new CborDecoder(byteArrayInputStream).decode();
                                byteArrayInputStream.close();
                                //CborDecoder.decode(Base64.decode(response.body().getData()));
                                for (final DataItem dataItem : decode) {
                                    LOG.info("Decoded data item: " + dataItem);
                                }
                                readStatisticObject.setEndTime(System.nanoTime());
                                done.complete(ImmutablePair.of(false, decode.toString()));
                            } else {
                                readStatisticObject.setEndTime(System.nanoTime());
                                done.complete(ImmutablePair.of(false, ""));
                            }
                        } catch (CborException | DecoderException | IOException ex) {
                            LOG.error("Cbor|Decoder Exception thrown");
                            ExceptionHandler.logException(ex);
                            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            done.complete(ImmutablePair.of(true, ex.getMessage()));
                        }
                    } else {
                        LOG.error("Response body was null");
                        readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        done.complete(ImmutablePair.of(true, "Response body was null"));
                    }
                } else {
                    readStatisticObject.setEndTime(-1);
                    done.complete(ImmutablePair.of(false, "Sent async"));
                }
            }

            @Suspendable
            @Override
            public void onFailure(@NotNull final Call<SingleState> call, @NotNull final Throwable throwable) {
                LOG.error("Failure: " + throwable.getMessage());
                readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                done.complete(ImmutablePair.of(true, throwable.getMessage()));
            }
        });

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

}
