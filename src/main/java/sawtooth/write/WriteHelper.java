package sawtooth.write;

import co.paralleluniverse.fibers.Suspendable;
import org.zeromq.ZMQ;
import sawtooth.sdk.protobuf.ClientReceiptGetRequest;
import sawtooth.sdk.protobuf.Message;

import java.util.List;

public class WriteHelper {

    @Suspendable
    public void sendTransactionReceiptRequest(final ZMQ.Socket socket, final List<String> transactionIds,
                                              final String correlationId) {
        ClientReceiptGetRequest.Builder clientReceiptGetRequestBuilder =
                ClientReceiptGetRequest.newBuilder();

        transactionIds.forEach(clientReceiptGetRequestBuilder::addTransactionIds);

        ClientReceiptGetRequest clientReceiptGetRequest = clientReceiptGetRequestBuilder.build();

        Message clientReceiptGetRequestMessage =
                Message.newBuilder().setCorrelationId(correlationId).setMessageType(Message.MessageType.CLIENT_RECEIPT_GET_REQUEST).setContent(clientReceiptGetRequest.toByteString()).build();
        socket.send(clientReceiptGetRequestMessage.toByteArray());
    }

}
