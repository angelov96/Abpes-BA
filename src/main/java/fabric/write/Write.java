package fabric.write;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.configuration.Configuration;
import fabric.statistics.WriteStatisticObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.hyperledger.fabric.sdk.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hyperledger.fabric.sdk.Channel.DiscoveryOptions.createDiscoveryOptions;

public class Write implements IWritingMethod {

    private static final Logger LOG = Logger.getLogger(Write.class);

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> write(final E... params) {
        if (params.length == 5) {

            TransactionProposalRequest transactionProposalRequest = (TransactionProposalRequest) params[0];
            Channel channel = (Channel) params[1];
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[2];
            List<Peer> peerList = (List<Peer>) params[3];
            List<Orderer> ordererList = (List<Orderer>) params[4];

            return write(transactionProposalRequest, channel, writeStatisticObject,
                    peerList, ordererList);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> write(final TransactionProposalRequest transactionProposalRequest,
                                                 final Channel channel,
                                                 final WriteStatisticObject writeStatisticObject,
                                                 final List<Peer> peerList,
                                                 final List<Orderer> ordererList) {
        writeStatisticObject.setStartTime(System.nanoTime());

        try {

            //Utils.getDiscoveredPeers(channel);

            Collection<ProposalResponse> responses;
            responses = Configuration.USE_SERVICE_DISCOVERY ?
                    channel.sendTransactionProposalToEndorsers(transactionProposalRequest,
                            createDiscoveryOptions().setEndorsementSelector(Configuration.ENDORSEMENT_SELECTOR)
                                    .setForceDiscovery(Configuration.FORCE_DISCOVERY).ignoreEndpoints(Configuration.IGNORE_ENDPOINTS)) :
                    (peerList == null ? channel.sendTransactionProposal(transactionProposalRequest) :
                            channel.sendTransactionProposal(transactionProposalRequest, peerList));

            if (peerList != null) {
                for (final Peer peer : peerList) {
                    writeStatisticObject.getParticipatingServers().add(
                            peer.getName() + "|ProposalPeerCustom"
                    );
                }
            }

            responses.forEach(response -> {
                LOG.debug("Added peer: " + response.getPeer().getName());
                writeStatisticObject.getParticipatingServers().add(
                        response.getPeer().getName() + "|ProposalPeer");
            });

            for (final ProposalResponse response : Objects.requireNonNull(responses)) {
                if (response.getStatus() != ProposalResponse.Status.SUCCESS || !response.isVerified()) {
                    LOG.error("Invalid endorsement response");
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true, response.getMessage());
                }

                LOG.info("".equals(response.getProposalResponse().getResponse().getMessage()) ? "EMPTY_RESPONSE" :
                        response.getProposalResponse().getResponse().getMessage()
                                + " :Endorsement message");
                LOG.info("".equals(response.getProposalResponse().getResponse().getPayload().toString(Charset.defaultCharset())) ? "EMPTY_PAYLOAD" :
                        response.getProposalResponse().getResponse().getPayload().toString(Charset.defaultCharset()) + " " +
                                ":Endorsement payload");
            }

            if (Configuration.CHECK_PROPOSAL_CONSISTENCY_SET) {
                Collection<Set<ProposalResponse>> proposalConsistencySets =
                        SDKUtils.getProposalConsistencySets(responses);
                if (proposalConsistencySets.size() != 1) {
                    LOG.error("Expected only one set of consistent proposal responses but got " + proposalConsistencySets.size() + " , this" +
                            "could be induced by nondeterministic chaincode");
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true,
                            "Expected only one set of consistent proposal responses but got " + proposalConsistencySets.size() + " , this" +
                                    "could be induced by nondeterministic chaincode");
                }
            }

            //Utils.getDiscoveredOrderers(channel);

            CompletableFuture<BlockEvent.TransactionEvent> transactionEventCompletableFuture =
                    (ordererList == null ? channel.sendTransaction(responses) : channel.sendTransaction(responses
                            , ordererList));

            if (ordererList != null) {
                for (final Orderer orderer : ordererList) {
                    writeStatisticObject.getParticipatingServers().add(
                            orderer.getName() + "|OrdererPeerCustom"
                    );
                }
            }

            if (Configuration.SEND_WRITE_SYNC) {
                BlockEvent.TransactionEvent transactionEvent;
                try {
                transactionEvent = transactionEventCompletableFuture.get(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);
                } catch (TimeoutException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return new ImmutablePair<>(true, "TIMEOUT_EX");
                }
                addOrdererToStatistics(writeStatisticObject, transactionEventCompletableFuture);

                if (transactionEvent != null) {
                    LOG.info("Write, txid: " + transactionEvent.getTransactionID() + " peer: " + transactionEvent.getPeer().getName());
                    writeStatisticObject.setEndTime(System.nanoTime());
                } else {
                    LOG.error("Transaction event is null");
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true,
                            "Transaction event is null");
                }
            } else if (Configuration.SEND_WRITE_ASYNC) {
                LOG.debug("Sent async");
                writeStatisticObject.setEndTime(-1);
            } else {
                throw new NotYetImplementedException("Not yet implemented");
            }
            return ImmutablePair.of(false, "");
        } catch (/*todo specify concrete exception(s) ProposalException | InvalidArgumentException | ServiceDiscoveryException | InterruptedException | ExecutionException*/Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

    @Suspendable
    private void addOrdererToStatistics(final WriteStatisticObject writeStatisticObject,
                                        final CompletableFuture<BlockEvent.TransactionEvent> transactionEventCompletableFuture) throws InterruptedException, ExecutionException {
        if (Configuration.SET_ORDERER_FROM_METADATA_IN_STATISTICS) {
            String prefix = "-----BEGIN CERTIFICATE-----";
            String suffix = "-----END CERTIFICATE-----";
            Pattern pattern = Pattern.compile("(" + prefix + ".*?" + suffix + ")", Pattern.DOTALL);
            Matcher matcher =
                    pattern.matcher(transactionEventCompletableFuture.get().getBlockEvent().getBlock().getMetadata().toByteString().toStringUtf8());
            String orderer = null;
            while (matcher.find()) {
                orderer = matcher.group(1);
            }

            orderer = Objects.requireNonNull(orderer).replaceAll("\\n", System.lineSeparator());

            try {
                String ordererCa = FileUtils.readFileToString(new File(Configuration.ORDERER_CA_PEM)
                        , Charset.defaultCharset());

                Pattern patternOrderer = Pattern.compile
                        ("((.*" + System.lineSeparator() + "){1})" + orderer.replaceAll("\\+", "\\\\+"));

                Matcher matcherOrderer = patternOrderer.matcher(ordererCa);
                while (matcherOrderer.find()) {
                    String ordererAddress = matcherOrderer.group(1).split("/")[5] + "|OrdererPeer";
                    LOG.debug("Added orderer: " + ordererAddress);
                    writeStatisticObject.getParticipatingServers().add(
                            ordererAddress);
                }

            } catch (IOException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

}
