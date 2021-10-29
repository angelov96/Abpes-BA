package graphene.components;

import com.google.common.primitives.Bytes;
import cy.agorise.graphenej.errors.MalformedAddressException;
import graphene.configuration.Configuration;
import org.bitcoinj.core.Base58;
import org.bitcoinj.core.ECKey;
import org.spongycastle.crypto.digests.RIPEMD160Digest;

import java.util.Arrays;

/**
 * Class used to encapsulate address-related operations.
 */
public class Address {

    private final PublicKey publicKey;
    private final String prefix;

    public Address(final ECKey key) {
        this.publicKey = new PublicKey(key);
        this.prefix = Configuration.ADDRESS_PREFIX;
    }

    public Address(final ECKey key, final String prefix) {
        this.publicKey = new PublicKey(key);
        this.prefix = prefix;
    }

    public Address(final String address) throws MalformedAddressException {
        this.prefix = address.substring(0, Configuration.PREFIX_LENGTH);
        byte[] decoded = Base58.decode(address.substring(Configuration.PREFIX_LENGTH));
        byte[] pubKey = Arrays.copyOfRange(decoded, 0, decoded.length - Configuration.PREFIX_LENGTH);
        byte[] checksum = Arrays.copyOfRange(decoded, decoded.length - Configuration.PREFIX_LENGTH, decoded.length);
        publicKey = new PublicKey(ECKey.fromPublicOnly(pubKey));
        byte[] calculatedChecksum = calculateChecksum(pubKey);
        for (int i = 0; i < calculatedChecksum.length; i++) {
            if (checksum[i] != calculatedChecksum[i]) {
                throw new MalformedAddressException("Checksum error");
            }
        }
    }

    private byte[] calculateChecksum(byte[] data) {
        byte[] checksum = new byte[160 / 8];
        RIPEMD160Digest ripemd160Digest = new RIPEMD160Digest();
        ripemd160Digest.update(data, 0, data.length);
        ripemd160Digest.doFinal(checksum, 0);
        return Arrays.copyOfRange(checksum, 0, Configuration.PREFIX_LENGTH);
    }

    public PublicKey getPublicKey() {
        return this.publicKey;
    }

    @Override
    public String toString() {
        byte[] pubKey = this.publicKey.toBytes();
        byte[] checksum = calculateChecksum(pubKey);
        byte[] pubKeyChecksummed = Bytes.concat(pubKey, checksum);
        return this.prefix + Base58.encode(pubKeyChecksummed);
    }
}
