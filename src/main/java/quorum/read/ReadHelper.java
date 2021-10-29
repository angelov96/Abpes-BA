package quorum.read;

import co.paralleluniverse.fibers.Suspendable;
import com.esotericsoftware.minlog.Log;
import org.apache.log4j.Logger;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.datatypes.Type;
import org.web3j.protocol.core.methods.response.EthCall;
import quorum.payloads.IQuorumPayload;

import java.util.List;

public class ReadHelper {

    private static final Logger LOG = Logger.getLogger(ReadHelper.class);

    private ReadHelper() {
    }

    @Suspendable
    public static List<Type<?>> decodeReadCall(final EthCall response, final IQuorumPayload iQuorumPayload) {
        List<Type<?>> decodedTypes = FunctionReturnDecoder.decode(
                response.getValue(), iQuorumPayload.getOutputList());

        Log.info("Size of types to decode: " + decodedTypes.size());
        if (decodedTypes.size() == 0) {
            LOG.error("Size of decoded types equal 0, this might be an error");
        }
        for (final Type<?> type : decodedTypes) {
            Log.info("Type as string: " + type.getTypeAsString());
            Log.info("Value: " + type.getValue());
        }
        return decodedTypes;
    }

}
