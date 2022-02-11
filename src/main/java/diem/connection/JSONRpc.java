package diem.connection;

import co.paralleluniverse.fibers.Suspendable;
import com.diem.DiemClient;
import com.diem.jsonrpc.DiemJsonRpcClient;
import com.diem.types.ChainId;

public class JSONRpc {
    @Suspendable
    public static DiemClient createDiemClient(final String jsonRpcUrl, final ChainId chainId) {
        return new DiemJsonRpcClient(jsonRpcUrl, chainId);
    }
}
