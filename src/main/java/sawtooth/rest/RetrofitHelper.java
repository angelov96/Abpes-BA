package sawtooth.rest;

import co.paralleluniverse.fibers.Suspendable;
import retrofit2.converter.gson.GsonConverterFactory;

public class RetrofitHelper {

    private RetrofitHelper() {
    }

    @Suspendable
    public static retrofit2.Retrofit buildRetrofit(final String url) {
        return new retrofit2.Retrofit.Builder()
                .baseUrl(url)
                .addConverterFactory(GsonConverterFactory.create())
                .build();
    }
}
