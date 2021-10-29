package graphene.components;

import client.supplements.ExceptionHandler;
import com.google.common.primitives.Bytes;
import com.google.gson.*;
import cy.agorise.graphenej.Extensions;
import cy.agorise.graphenej.Util;
import cy.agorise.graphenej.errors.MalformedAddressException;
import cy.agorise.graphenej.interfaces.GrapheneSerializable;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Class used to represent the weighted set of keys and accounts that must approve operations.
 * <p>
 * {@see <a href="https://bitshares.org/doxygen/structgraphene_1_1chain_1_1authority.html">Authority</a>}
 */
public class Authority implements GrapheneSerializable {
    public static final String KEY_ACCOUNT_AUTHS = "account_auths";
    public static final String KEY_KEY_AUTHS = "key_auths";
    public static final String KEY_WEIGHT_THRESHOLD = "weight_threshold";
    public static final String KEY_EXTENSIONS = "extensions";
    private final Extensions extensions;
    private long weight_threshold;
    private Map<UserAccount, Long> account_auths;
    private Map<PublicKey, Long> key_auths;

    /**
     * Constructor for the authority class that takes every possible detail.
     *
     * @param weight_threshold: The total weight threshold
     * @param keyAuths:         Map of key to weights relationships. Can be null.
     * @param accountAuths:     Map of account to weights relationships. Can be null.
     * @throws MalformedAddressException
     */
    public Authority(final long weight_threshold, final Map<PublicKey, Long> keyAuths, final Map<UserAccount,
            Long> accountAuths) {
        this();
        this.weight_threshold = weight_threshold;
        this.key_auths = keyAuths;
        this.account_auths = accountAuths;
    }

    public Authority() {
        this.weight_threshold = 1;
        this.account_auths = new HashMap<>();
        this.key_auths = new HashMap<>();
        extensions = new Extensions();
    }

    public long getWeightThreshold() {
        return weight_threshold;
    }

    public void setWeightThreshold(final long weight_threshold) {
        this.weight_threshold = weight_threshold;
    }

    public void setKeyAuthorities(final Map<Address, Long> keyAuths) {
        if (keyAuths != null) {
            for (final Address address : keyAuths.keySet()) {
                key_auths.put(address.getPublicKey(), keyAuths.get(address));
            }
        }
    }

    public void setAccountAuthorities(final Map<UserAccount, Long> accountAuthorities) {
        this.account_auths = accountAuthorities;
    }

    /**
     * @return Returns a list of public keys linked to this authority
     */
    public List<PublicKey> getKeyAuthList() {
        return new ArrayList<>(key_auths.keySet());
    }

    /**
     * @return Returns a list of accounts linked to this authority
     */
    public List<UserAccount> getAccountAuthList() {
        return new ArrayList<>(account_auths.keySet());
    }

    @Override
    public String toJsonString() {
        return null;
    }

    @Override
    public JsonElement toJsonObject() {
        JsonObject authority = new JsonObject();
        authority.addProperty(KEY_WEIGHT_THRESHOLD, weight_threshold);
        JsonArray keyAuthArray = new JsonArray();
        JsonArray accountAuthArray = new JsonArray();

        for (final PublicKey publicKey : key_auths.keySet()) {
            JsonArray subArray = new JsonArray();
            Address address = new Address(publicKey.getKey());
            subArray.add(address.toString());
            subArray.add(key_auths.get(publicKey));
            keyAuthArray.add(subArray);
        }

        for (final UserAccount key : account_auths.keySet()) {
            JsonArray subArray = new JsonArray();
            subArray.add(key.toString());
            subArray.add(key_auths.get(key));
            accountAuthArray.add(subArray);
        }
        authority.add(KEY_KEY_AUTHS, keyAuthArray);
        authority.add(KEY_ACCOUNT_AUTHS, accountAuthArray);
        authority.add(KEY_EXTENSIONS, extensions.toJsonObject());
        return authority;
    }

    @Override
    public byte[] toBytes() {
        List<Byte> byteArray = new ArrayList<>();
        // Adding number of authorities
        byteArray.add((byte) (account_auths.size() + key_auths.size()));

        // If the authority is not empty of references, we serialize its contents
        // otherwise its only contribution will be a zero byte
        if (account_auths.size() + key_auths.size() > 0) {
            // Weight threshold
            byteArray.addAll(Bytes.asList(Util.revertInteger((int) weight_threshold)));

            // Number of account authorities
            byteArray.add((byte) account_auths.size());

            //TODO: Check the account authorities serialization
            // Serializing individual accounts and their corresponding weights
            for (final UserAccount account : account_auths.keySet()) {
                byteArray.addAll(Bytes.asList(account.toBytes()));
                byteArray.addAll(Bytes.asList(Util.revertShort(account_auths.get(account).shortValue())));
            }

            // Number of key authorities
            byteArray.add((byte) key_auths.size());

            // Serializing individual keys and their corresponding weights
            for (final PublicKey publicKey : key_auths.keySet()) {
                byteArray.addAll(Bytes.asList(publicKey.toBytes()));
                byteArray.addAll(Bytes.asList(Util.revertShort(key_auths.get(publicKey).shortValue())));
            }

            // Adding number of extensions
            byteArray.add((byte) extensions.size());
        }
        return Bytes.toArray(byteArray);
    }

    @Override
    public int hashCode() {
        return Objects.hash(weight_threshold, account_auths, key_auths, extensions);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj instanceof Authority) {
            Authority authority = (Authority) obj;
            Map<PublicKey, Long> keyAuths = authority.getKeyAuths();
            Map<UserAccount, Long> accountAuths = authority.getAccountAuths();
            System.out.println("key auths match: " + this.key_auths.equals(keyAuths));
            System.out.println("account auths match: " + this.account_auths.equals(accountAuths));
            System.out.println("weight threshold matches: " + (this.weight_threshold == authority.weight_threshold));
            return this.key_auths.equals(keyAuths) &&
                    this.account_auths.equals(accountAuths) &&
                    this.weight_threshold == authority.weight_threshold;
        }
        return false;
    }

    public Map<PublicKey, Long> getKeyAuths() {
        return this.key_auths;
    }

    public Map<UserAccount, Long> getAccountAuths() {
        return this.account_auths;
    }

    /**
     * Custom deserializer used while parsing the 'get_account_by_name' API call response.
     * <p>
     * This will deserialize an account authority in the form:
     * <p>
     * {
     * "weight_threshold": 1,
     * "account_auths": [],
     * "key_auths": [["BTS6yoiaoC4p23n31AV4GnMy5QDh5yUQEUmU4PmNxRQPGg7jjPkBq",1]],
     * "address_auths": []
     * }
     */
    public static class AuthorityDeserializer implements JsonDeserializer<Authority> {

        @Override
        public Authority deserialize(final JsonElement json, final Type typeOfT,
                                     final JsonDeserializationContext context) throws JsonParseException {
            JsonObject baseObject = json.getAsJsonObject();
            long weightThreshold = baseObject.get(KEY_WEIGHT_THRESHOLD).getAsLong();
            JsonArray keyAuthArray = baseObject.getAsJsonArray(KEY_KEY_AUTHS);
            JsonArray accountAuthArray = baseObject.getAsJsonArray(KEY_ACCOUNT_AUTHS);
            HashMap<PublicKey, Long> keyAuthMap = new HashMap<>();
            HashMap<UserAccount, Long> accountAuthMap = new HashMap<>();
            for (int i = 0; i < keyAuthArray.size(); i++) {
                JsonArray subArray = keyAuthArray.get(i).getAsJsonArray();
                String addr = subArray.get(0).getAsString();
                long weight = subArray.get(1).getAsLong();
                try {
                    keyAuthMap.put(new Address(addr).getPublicKey(), weight);
                } catch (MalformedAddressException ex) {
                    ExceptionHandler.logException(ex);
                }
            }
            for (int i = 0; i < accountAuthArray.size(); i++) {
                JsonArray subArray = accountAuthArray.get(i).getAsJsonArray();
                String userId = subArray.get(0).getAsString();
                long weight = subArray.get(1).getAsLong();
                UserAccount userAccount = new UserAccount(userId);
                accountAuthMap.put(userAccount, weight);
            }
            return new Authority(weightThreshold, keyAuthMap, accountAuthMap);
        }
    }
}