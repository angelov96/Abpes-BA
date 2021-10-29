package sawtooth.helper;

import co.nstant.in.cbor.model.*;
import co.paralleluniverse.fibers.Suspendable;
import cy.agorise.graphenej.Util;
import org.apache.log4j.Logger;
import org.bouncycastle.jcajce.provider.digest.SHA512;
import sawtooth.endpointdata.batches.Batch;
import sawtooth.endpointdata.batches.Datum;
import sawtooth.endpointdata.batches.Transaction;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

public class SawtoothHelper {

    private static final Logger LOG = Logger.getLogger(SawtoothHelper.class);

    private SawtoothHelper() {
    }

    @Suspendable
    public static String createSha512HashAsString(final String valToHash) {
        return Util.bytesToHex(getDigest(valToHash.getBytes()));
    }

    @Suspendable
    private static byte[] getDigest(final byte[] bytes) {
        SHA512.Digest digest = new SHA512.Digest();
        return digest.digest(bytes);
    }

    @Suspendable
    public static String createSha512HashAsByteArray(final ByteArrayOutputStream valToHash) {
        return Util.bytesToHex(getDigest(valToHash.toByteArray()));
    }

    @Suspendable
    public static void printBatchListResponseFull(final Batch batchResponseFull) {
        boolean pagingNotNull = batchResponseFull.getPaging() == null;
        boolean pagingLimitNotNull = !pagingNotNull && batchResponseFull.getPaging().getLimit() != null;
        boolean pagingStartNotNull = !pagingNotNull && batchResponseFull.getPaging().getStart() != null;

        LOG.info("Head: " + batchResponseFull.getHead() + "\n" +
                "Link:" + batchResponseFull.getLink() + "\n" +
                "Paging Limit:" + (pagingLimitNotNull ? batchResponseFull.getPaging().getLimit() : "null") + "\n" +
                "Paging Start:" + (pagingStartNotNull ? batchResponseFull.getPaging().getStart() : "null"));

        if (batchResponseFull.getData() != null) {
            for (final Datum datum : batchResponseFull.getData()) {
                LOG.info("1. Signer Public key:" + datum.getHeader().getSignerPublicKey());
                for (final String transactionId : datum.getHeader().getTransactionIds()) {
                    LOG.info("2. Transaction id: " + transactionId);
                }

                LOG.info("3. Header signature" + datum.getHeaderSignature());
                LOG.info("4. Trace: " + datum.isTrace());
                for (final Transaction transaction : datum.getTransactions()) {
                    LOG.info("5. Transaction batcher public key: " + transaction.getHeader().getBatcherPublicKey());
                    for (final Object dependency : transaction.getHeader().getDependencies()) {
                        LOG.info("6. Transaction dependency:" + dependency);
                    }
                    LOG.info("7. Transaction family name: " + transaction.getHeader().getFamilyName());
                    LOG.info("8. Transaction family version: " + transaction.getHeader().getFamilyVersion());
                    for (final String input : transaction.getHeader().getInputs()) {
                        LOG.info("9. Transaction input: " + input);
                    }
                    LOG.info("10. Transaction nonce: " + transaction.getHeader().getNonce());
                    for (final String output : transaction.getHeader().getOutputs()) {
                        LOG.info("11. Transaction output: " + output);
                    }
                    LOG.info("12. Transaction payload sha512: " + transaction.getHeader().getPayloadSha512());
                    LOG.info("13. Transaction signer public key:" + transaction.getHeader().getSignerPublicKey());
                    LOG.info("14. Transaction header signature: " + transaction.getHeaderSignature());

                    LOG.info("15. Transaction payload: " + transaction.getPayload());

                }
            }
        }
    }

    public static <E> E decodeCbor(final DataItem dataItem) {
        E retVal = null;

        switch (dataItem.getMajorType()) {
            case MAP:
                co.nstant.in.cbor.model.Map map = (co.nstant.in.cbor.model.Map) dataItem;
                for (final DataItem key : map.getKeys()) {
                    LOG.debug("Key: " + key + " Value: " + map.get(key));
                }
                retVal = (E) map;
                break;
            case ARRAY:
                Array array = (Array) dataItem;
                for (final DataItem item : array.getDataItems()) {
                    LOG.debug("Array item: " + item);
                }
                retVal = (E) array;
                break;
            case BYTE_STRING:
                ByteString byteString = (ByteString) dataItem;
                LOG.debug("ByteString: " + Arrays.toString(byteString.getBytes()));
                retVal = (E) byteString.getBytes();
                break;
            case UNICODE_STRING:
                UnicodeString unicodeString = (UnicodeString) dataItem;
                LOG.debug("Unicode string: " + unicodeString.getString());
                retVal = (E) unicodeString.getString();
                break;
            case UNSIGNED_INTEGER:
                UnsignedInteger unsignedInteger = (UnsignedInteger) dataItem;
                LOG.debug("Unsigned integer: " + unsignedInteger);
                retVal = (E) unsignedInteger.getValue();
                break;
            case NEGATIVE_INTEGER:
                NegativeInteger negativeInteger = (NegativeInteger) dataItem;
                LOG.debug("Negative integer: " + negativeInteger);
                retVal = (E) negativeInteger.getValue();
                break;
            default:
                LOG.error("Cbor type not yet implemented: " + dataItem.getMajorType());
                break;
        }

        return retVal;
    }
}
