package graphene.components;

import com.google.common.primitives.Bytes;
import com.google.gson.*;
import cy.agorise.graphenej.Extensions;
import cy.agorise.graphenej.Util;
import cy.agorise.graphenej.Vote;
import cy.agorise.graphenej.errors.MalformedAddressException;
import cy.agorise.graphenej.interfaces.GrapheneSerializable;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by nelson on 12/5/16.
 */
public class AccountOptions implements GrapheneSerializable {

    public static final String KEY_MEMO_KEY = "memo_key";
    public static final String KEY_NUM_COMMITTEE = "num_committee";
    public static final String KEY_NUM_WITNESS = "num_witness";
    public static final String KEY_VOTES = "votes";
    public static final String KEY_VOTING_ACCOUNT = "voting_account";
    public static final String KEY_EXTENSIONS = Extensions.KEY_EXTENSIONS;
    private static final Logger LOG = Logger.getLogger(AccountOptions.class);
    private final Extensions extensions;
    private PublicKey memo_key;
    private UserAccount voting_account;
    private int num_witness;
    private int num_committee;
    private Vote[] votes;

    /**
     * Constructor used to instantiate only the following attributes:
     * <ul>
     *     <li>voting_account</li>
     *     <li>votes</li>
     *     <li>memo_key</li>
     *     <li>extensions</li>
     * </ul>
     */
    public AccountOptions(final PublicKey memoKey) {
        this();
        this.memo_key = memoKey;
    }

    /**
     * Constructor used to instantiate only the following attributes:
     * <ul>
     *     <li>voting_account</li>
     *     <li>votes</li>
     *     <li>extensions</li>
     * </ul>
     */
    public AccountOptions() {
        voting_account = new UserAccount(UserAccount.PROXY_TO_SELF);
        this.votes = new Vote[0];
        this.extensions = new Extensions();
    }

    /**
     * Constructor that can be used to instantiate a version of the AccountOptions object
     * with a null reference in the 'voting_account' attribute. This can be used to prevent
     * a circular dependency situation when de-serializing the UserAccount instance.
     *
     * @param memoKey        Memo public key used by this account
     * @param includeAccount Whether or not to instantiate an UserAccount
     */
    public AccountOptions(final PublicKey memoKey, final boolean includeAccount) {
        if (includeAccount) {
            voting_account = new UserAccount(UserAccount.PROXY_TO_SELF);
        }
        this.memo_key = memoKey;
        this.votes = new Vote[0];
        this.extensions = new Extensions();
    }

    //TODO: Implement constructor that takes a Vote array.
    public AccountOptions(final PublicKey memoKey, final boolean includeAccount, final Vote[] votes) {
        if (includeAccount) {
            voting_account = new UserAccount(UserAccount.PROXY_TO_SELF);
        }
        this.memo_key = memoKey;
        this.votes = votes;
        this.extensions = new Extensions();
    }

    public PublicKey getMemoKey() {
        return memo_key;
    }

    public void setMemoKey(final PublicKey memo_key) {
        this.memo_key = memo_key;
    }

    public UserAccount getVotingAccount() {
        return voting_account;
    }

    public void setVotingAccount(final UserAccount voting_account) {
        this.voting_account = voting_account;
    }

    public int getNumWitness() {
        return num_witness;
    }

    public void setNumWitness(final int num_witness) {
        this.num_witness = num_witness;
    }

    public int getNumCommittee() {
        return num_committee;
    }

    public void setNum_committee(final int num_committee) {
        this.num_committee = num_committee;
    }

    public Vote[] getVotes() {
        return votes;
    }

    public void setVotes(final Vote[] votes) {
        this.votes = votes;
    }

    @Override
    public byte[] toBytes() {
        List<Byte> byteArray = new ArrayList<>();

        if (memo_key != null) {
            // Adding byte to indicate that there is memo data
            byteArray.add((byte) 1);

            // Adding memo key
            byteArray.addAll(Bytes.asList(memo_key.toBytes()));

            // Adding voting account
            byteArray.addAll(Bytes.asList(voting_account.toBytes()));

            // Adding num_witness
            byteArray.addAll(Bytes.asList(Util.revertShort((short) num_witness)));

            // Adding num_committee
            byteArray.addAll(Bytes.asList(Util.revertShort((short) num_committee)));

            // Vote's array length
            byteArray.add((byte) votes.length);

            for (final Vote vote : votes) {
                //TODO: Check this serialization
                byteArray.addAll(Bytes.asList(vote.toBytes()));
            }

            // Account options's extensions
            byteArray.addAll(Bytes.asList(extensions.toBytes()));
        } else {
            byteArray.add((byte) 0);
        }
        return Bytes.toArray(byteArray);
    }

    @Override
    public String toJsonString() {
        return null;
    }

    @Override
    public JsonElement toJsonObject() {
        JsonObject options = new JsonObject();
        options.addProperty(KEY_MEMO_KEY, new Address(memo_key.getKey()).toString());
        options.addProperty(KEY_NUM_COMMITTEE, num_committee);
        options.addProperty(KEY_NUM_WITNESS, num_witness);
        options.addProperty(KEY_VOTING_ACCOUNT, voting_account.getObjectId());
        JsonArray votesArray = new JsonArray();
        for (final Vote vote : votes) {
            //TODO: Add votes representation
        }
        options.add(KEY_VOTES, votesArray);
        options.add(KEY_EXTENSIONS, extensions.toJsonObject());
        return options;
    }

    /**
     * Custom deserializer used while parsing the 'get_account_by_name' API call response.
     * TODO: Implement all other details besides the key
     */
    public static class AccountOptionsDeserializer implements JsonDeserializer<AccountOptions> {

        private final boolean mIncludeUserAccount;

        public AccountOptionsDeserializer() {
            this.mIncludeUserAccount = true;
        }

        public AccountOptionsDeserializer(boolean includeUserAccount) {
            this.mIncludeUserAccount = includeUserAccount;
        }

        @Override
        public AccountOptions deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            JsonObject baseObject = json.getAsJsonObject();
            AccountOptions options;
            try {
                Address address = new Address(baseObject.get(KEY_MEMO_KEY).getAsString());
                options = new AccountOptions(address.getPublicKey(), mIncludeUserAccount);
            } catch (IllegalArgumentException | MalformedAddressException ex) {
                LOG.error("Caught exception while deserializing account options");
                options = new AccountOptions();

            }
            return options;
        }
    }
}
