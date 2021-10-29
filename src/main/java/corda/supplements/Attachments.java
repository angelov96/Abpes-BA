package corda.supplements;

import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import net.corda.core.crypto.SecureHash;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.node.services.vault.AttachmentQueryCriteria;
import net.corda.core.node.services.vault.AttachmentSort;

import java.io.*;
import java.util.List;

public class Attachments {

    @Suspendable
    public List<SecureHash> queryAttachment(final CordaRPCOps proxy,
                                            final AttachmentQueryCriteria attachmentQueryCriteria,
                                            final AttachmentSort attachmentSort) {
        return proxy.queryAttachments(attachmentQueryCriteria,
                attachmentSort);
    }

    @Suspendable
    public boolean checkAttachmentExists(final CordaRPCOps proxy, final String secureHash) {
        return proxy.attachmentExists(SecureHash.parse(secureHash));
    }

    @Suspendable
    public SecureHash uploadAttachment(final CordaRPCOps proxy, final String file) {
        try (InputStream attachment = new FileInputStream(file)) {
            return proxy.uploadAttachment(attachment);
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return null;
    }

    @Suspendable
    public SecureHash uploadAttachmentWithMetadata(final CordaRPCOps proxy, final String file, final String uploader,
                                                   final String fileName) {
        try (InputStream attachment = new FileInputStream(file)) {
            return proxy.uploadAttachmentWithMetadata(attachment, uploader, fileName);
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
        return null;
    }

    @Suspendable
    public InputStream openAttachment(final CordaRPCOps proxy, final String secureHash) {
        return proxy.openAttachment(SecureHash.parse(
                secureHash));
    }

    @Suspendable
    public void saveAttachment(final InputStream inputStream, final String targetFile) {
        try {
            byte[] buffer = new byte[inputStream.available()];

            File targetFileToWrite = new File(targetFile);
            try (OutputStream outStream = new FileOutputStream(targetFileToWrite)) {
                outStream.write(buffer);
            }
        } catch (IOException ex) {
            ExceptionHandler.logException(ex);
        }
    }

}
