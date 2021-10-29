package sawtooth.connection;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.strands.Strand;
import org.apache.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import sawtooth.configuration.Configuration;

public class ZmqConnection {

    private static final Logger LOG = Logger.getLogger(ZmqConnection.class);

    @Suspendable
    public static void disconnectSocket(final ZMQ.Socket socket, final String address) {
        socket.disconnect(address);
    }

    @Suspendable
    public static void closeSocket(final ZMQ.Socket socket) {
        socket.close();
    }

    @Suspendable
    public ZMQ.Socket createZmqListener() {
        ZMQ.Context context = ZMQ.context(Configuration.ZMQ_IO_THREADS);
        return context.socket(SocketType.DEALER);
    }

    @Suspendable
    public void setZmqCurveKeys(final ZMQ.Socket socket) {
        socket.setCurvePublicKey(Configuration.ZMQ_CURVE_PUBLIC_KEY.getBytes());
        socket.setCurveSecretKey(Configuration.ZMQ_CURVE_PRIVATE_KEY.getBytes());
    }

    @Suspendable
    public void connectToZmq(final ZMQ.Socket socket, final String address) {
        boolean hasError;
        int i = 0;
        do {
            boolean connectedToSocket = socket.connect(address);
            LOG.debug("Connected to socket: " + connectedToSocket + " retry: " + i);
            socket.setTCPKeepAlive(Configuration.ZMQ_TCP_KEEP_ALIVE);
            socket.setTCPKeepAliveCount(Configuration.ZMQ_TCP_KEEP_ALIVE_COUNT);
            socket.setReceiveTimeOut(Configuration.ZMQ_RECEIVE_TIMEOUT);
            socket.setSendTimeOut(Configuration.ZMQ_SEND_TIMEOUT);
            socket.setReceiveBufferSize(
                    Configuration.ZMQ_RECEIVE_BUFFER_SIZE == -1 ?
                            socket.getReceiveBufferSize() : Configuration.ZMQ_RECEIVE_BUFFER_SIZE);
            socket.setSendBufferSize(Configuration.ZMQ_SEND_BUFFER_SIZE == -1 ?
                    socket.getSendBufferSize() : Configuration.ZMQ_SEND_BUFFER_SIZE);
            hasError = !connectedToSocket;

            if (hasError) {
                try {
                    Strand.sleep(Configuration.WEBSOCKET_RECONNECTION_SLEEP_TIME);
                } catch (SuspendExecution | InterruptedException exception) {
                    ExceptionHandler.logException(exception);
                } finally {
                    i++;
                }
            }
        } while (hasError && i < Configuration.MAX_CONNECTION_RETRIES);
    }

}
