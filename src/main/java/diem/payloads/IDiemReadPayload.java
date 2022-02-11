package diem.payloads;

import client.commoninterfaces.IBlockchainPayload;

public interface IDiemReadPayload {
    IBlockchainPayload.Payload_Type PAYLOAD_TYPE = IBlockchainPayload.Payload_Type.READ;
}
