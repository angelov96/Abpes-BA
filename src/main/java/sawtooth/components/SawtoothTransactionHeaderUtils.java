package sawtooth.components;

import co.paralleluniverse.fibers.Suspendable;
import org.apache.commons.lang3.RandomStringUtils;
import sawtooth.helper.SawtoothHelper;
import sawtooth.sdk.protobuf.TransactionHeader;
import sawtooth.sdk.signing.Signer;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.UUID;

public class SawtoothTransactionHeaderUtils {

    private SawtoothTransactionHeaderUtils() {
    }

    @Suspendable
    public static TransactionHeader buildTransactionHeader(final Signer signer, final ByteArrayOutputStream payload,
                                                           final List<String> inputList,
                                                           final List<String> outputList, final String familyName,
                                                           final String familyVersion) {
        TransactionHeader.Builder transactionHeaderBuilder = TransactionHeader.newBuilder()
                .setSignerPublicKey(signer.getPublicKey().hex())
                .setFamilyName(familyName)
                .setFamilyVersion(familyVersion)
                .setPayloadSha512(SawtoothHelper.createSha512HashAsByteArray(payload))
                .setBatcherPublicKey(signer.getPublicKey().hex())
                .setNonce(UUID.randomUUID().toString() + RandomStringUtils.random(10, true, true) + System.nanoTime());

        inputList.forEach(transactionHeaderBuilder::addInputs);

        outputList.forEach(transactionHeaderBuilder::addOutputs);

        return transactionHeaderBuilder.build();
    }

}
