package client.utils;

import co.paralleluniverse.fibers.Suspendable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class ThreadPoolFactoryFactoryFacade {

    @Suspendable
    public static ThreadFactory setThreadPoolFactoryWithName(final String name) {
        return new ThreadFactoryBuilder().setThreadFactory(Executors.defaultThreadFactory()).setNameFormat(name).build();
    }

}
