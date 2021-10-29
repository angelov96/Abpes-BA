package graphene.components;

import cy.agorise.graphenej.interfaces.ByteSerializable;
import org.bitcoinj.core.ECKey;

import java.io.Serializable;

/**
 * Created by nelson on 11/30/16.
 */
public class PublicKey implements ByteSerializable, Serializable {
    private ECKey publicKey;

    public PublicKey(final ECKey key) {
        if (key.hasPrivKey()) {
            throw new IllegalStateException("Passing a private key to PublicKey constructor");
        }
        this.publicKey = key;
    }

    @Override
    public byte[] toBytes() {
        if (publicKey.isCompressed()) {
            return publicKey.getPubKey();
        } else {
            publicKey = ECKey.fromPublicOnly(publicKey.getPubKeyPoint(), false);
                    //ECKey.compressPoint(publicKey.getPubKeyPoint()));
            return publicKey.getPubKey();
        }
    }

    @Override
    public int hashCode() {
        return publicKey.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PublicKey) {
            PublicKey other = (PublicKey) obj;
            return this.publicKey.equals(other.getKey());
        }
        return false;
    }

    public ECKey getKey() {
        return publicKey;
    }

    @Override
    public String toString() {
        return getAddress();
    }

    public String getAddress() {
        ECKey pk = ECKey.fromPublicOnly(publicKey.getPubKey());
        if (!pk.isCompressed()) {
            /*ECPoint point = ECKey.compressPoint(pk.getPubKeyPoint());
            pk = ECKey.fromPublicOnly(point);*/
            pk = ECKey.fromPublicOnly(pk.getPubKeyPoint(), false);
        }
        return new Address(pk).toString();
    }
}