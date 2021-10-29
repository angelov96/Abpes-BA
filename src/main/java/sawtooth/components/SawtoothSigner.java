package sawtooth.components;

import co.paralleluniverse.fibers.Suspendable;
import sawtooth.sdk.signing.PrivateKey;
import sawtooth.sdk.signing.Secp256k1Context;
import sawtooth.sdk.signing.Signer;

public class SawtoothSigner {

    private final Signer sawtoothSigner;
    private final String clientId;

    public SawtoothSigner(final String clientIdConstructor) {
        sawtoothSigner = createSigner();
        clientId = clientIdConstructor;
    }

    @Suspendable
    private Signer createSigner() {
        Secp256k1Context context = new Secp256k1Context();
        PrivateKey privateKey = context.newRandomPrivateKey();
        return new Signer(context, privateKey);
    }

    public String getClientId() {
        return clientId;
    }

    public Signer getSigner() {
        return sawtoothSigner;
    }

}
