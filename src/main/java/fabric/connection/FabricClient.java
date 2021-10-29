package fabric.connection;

import client.supplements.ExceptionHandler;
import client.utils.ThreadPoolFactoryFactoryFacade;
import fabric.configuration.Configuration;
import org.apache.log4j.Logger;
import org.hyperledger.fabric.sdk.Channel;
import org.hyperledger.fabric.sdk.HFClient;
import org.hyperledger.fabric.sdk.User;
import org.hyperledger.fabric.sdk.exception.CryptoException;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.security.CryptoSuite;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.*;

public class FabricClient {

    private static final Logger LOG = Logger.getLogger(FabricClient.class);

    private HFClient instance;

    public FabricClient(final User context) {
        try {
            CryptoSuite cryptoSuite = CryptoSuite.Factory.getCryptoSuite();
            instance = HFClient.createNewInstance();

            Field executorService = HFClient.class.getDeclaredField("executorService");
            executorService.setAccessible(true);
            Method method = Channel.class.getDeclaredMethod("startEventQue");
            method.setAccessible(true);

            executorService.set(instance, newThreadPool(pool -> {
                Thread thread = ThreadPoolFactoryFactoryFacade.setThreadPoolFactoryWithName("fabric-client-pool-%d").newThread(pool);
                thread.setDaemon(Configuration.SET_DAEMON);
                return thread;
            }));
            LOG.info("Reflection succeeded");

            instance.setCryptoSuite(cryptoSuite);
            instance.setUserContext(context);
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | CryptoException | InvalidArgumentException | NoSuchMethodException | InvocationTargetException | NoSuchFieldException ex) {
            LOG.error("Reflection failed: " + ex.getMessage());
            ExceptionHandler.logException(ex, false);
        }
    }

    private static ExecutorService newThreadPool(final ThreadFactory threadFactory) {
        ThreadPoolExecutor threadPoolExecutor = null;
        try {
            threadPoolExecutor = new ThreadPoolExecutor(Configuration.CORE_POOL_SIZE,
                    Configuration.MAXIMUM_POOL_SIZE,
                    Configuration.KEEP_ALIVE_TIME, Configuration.TIME_UNIT,
                    (BlockingQueue<Runnable>) Configuration.ABSTRACT_QUEUE.getDeclaredConstructor().newInstance(),
                    threadFactory);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException ex) {
            ExceptionHandler.logException(ex);
        }
        Objects.requireNonNull(threadPoolExecutor).allowCoreThreadTimeOut(Configuration.ALLOW_CORE_THREAD_TIME_OUT);
        return threadPoolExecutor;
    }

    public HFClient getInstance() {
        return instance;
    }

    public ChannelClient createChannelClient(final String name) throws InvalidArgumentException {
        Channel channel = instance.newChannel(name);
        return new ChannelClient(name, channel);
    }

}
