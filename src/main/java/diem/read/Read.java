package diem.read;

import com.diem.DiemClient;
import fabric.payloads.IFabricReadPayload;
import fabric.statistics.ReadStatisticObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hibernate.cfg.NotYetImplementedException;
import org.hyperledger.fabric.sdk.Orderer;
import org.hyperledger.fabric.sdk.Peer;

import java.util.List;

public class Read implements IReadingMethod {


    @Override
    public <E> ImmutablePair<Boolean, String> read(E... params) {
        if (params.length == 5) {
            DiemClient fabricClient = (DiemClient) params[0];
            IDiemReadPayload fabricChainCodeObject = (IFabricReadPayload) params[2];
            fabric.statistics.ReadStatisticObject readStatisticObject = (ReadStatisticObject) params[3];
            List<Peer> peerList = (List<Peer>) params[4];
            List<Orderer> ordererList = (List<Orderer>) params[5];

            return read(fabricClient, fabricChainCodeObject, readStatisticObject,
                    peerList,
                    ordererList);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }
}
