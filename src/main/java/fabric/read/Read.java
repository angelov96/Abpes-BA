package fabric.read;

import client.configuration.GeneralConfiguration;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import fabric.configuration.Configuration;
import fabric.connection.FabricClient;
import fabric.payloads.IFabricReadPayload;
import fabric.statistics.ReadStatisticObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;
import org.hyperledger.fabric.sdk.exception.ServiceDiscoveryException;

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

public class Read implements IReadingMethod {

    private static final Logger LOG = Logger.getLogger(Read.class);

    @SafeVarargs
    @Suspendable
    @Override
    public final <E> ImmutablePair<Boolean, String> read(final E... params) {
        if (params.length == 6) {
            FabricClient fabricClient = (FabricClient) params[0];
            Channel channel = (Channel) params[1];
            IFabricReadPayload fabricChainCodeObject = (IFabricReadPayload) params[2];
            fabric.statistics.ReadStatisticObject readStatisticObject = (ReadStatisticObject) params[3];
            List<Peer> peerList = (List<Peer>) params[4];
            List<Orderer> ordererList = (List<Orderer>) params[5];

            return read(fabricClient, channel, fabricChainCodeObject, readStatisticObject,
                    peerList,
                    ordererList);
        } else {
            throw new NotYetImplementedException("Not yet implemented, extend as needed");
        }
    }

    @Suspendable
    private ImmutablePair<Boolean, String> read(final FabricClient fabricClient, final Channel channel,
                                                final IFabricReadPayload iFabricReadPayload,
                                                final ReadStatisticObject readStatisticObject,
                                                final List<Peer> peerList,
                                                final List<Orderer> ordererList) {

        readStatisticObject.setStartTime(System.nanoTime());

        try {
            //Utils.getDiscoveredPeers(channel);

            TransactionProposalRequest transactionRequest = iFabricReadPayload.getTransactionRequest(fabricClient);

            Collection<ProposalResponse> responses = Configuration.USE_SERVICE_DISCOVERY ?
                    channel.sendTransactionProposalToEndorsers(transactionRequest,
                            createDiscoveryOptions().setEndorsementSelector(Configuration.ENDORSEMENT_SELECTOR)
                                    .setForceDiscovery(Configuration.FORCE_DISCOVERY).ignoreEndpoints(Configuration.IGNORE_ENDPOINTS)) :
                    (peerList == null ? channel.sendTransactionProposal(transactionRequest) :
                            channel.sendTransactionProposal(transactionRequest, peerList));

            if (peerList != null) {
                for (final Peer peer : peerList) {
                    readStatisticObject.getParticipatingServers().add(
                            peer.getName() + "|ProposalPeerCustom"
                    );
                }
            }

            responses.forEach(response -> {
                LOG.debug("Added peer: " + response.getPeer().getName());
                readStatisticObject.getParticipatingServers().add(
                        response.getPeer().getName() + "|ProposalPeer");
            });

            if (Configuration.RECEIVE_READ_REQUEST) {
                for (final ProposalResponse response : responses) {
                    LOG.info("Endorsement message: " + ("".equals(response.getProposalResponse().getResponse().getMessage())
                            ? "EMPTY_RESPONSE" :
                            response.getProposalResponse().getResponse().getMessage()));
                    LOG.info("Endorsement payload: " + ("".equals(response.getProposalResponse().getResponse().getPayload().toString(Charset.defaultCharset())) ? "EMPTY_PAYLOAD" :
                            response.getProposalResponse().getResponse().getPayload().toString(Charset.defaultCharset())));
                    if (response.getStatus() != ProposalResponse.Status.SUCCESS || !response.isVerified()) {
                        LOG.error("Invalid endorsement response");
                    }
                }

                if (Configuration.CHECK_PROPOSAL_CONSISTENCY_SET) {
                    Collection<Set<ProposalResponse>> proposalConsistencySets =
                            SDKUtils.getProposalConsistencySets(responses);
                    if (proposalConsistencySets.size() != 1) {
                        LOG.error("Expected only one set of consistent proposal responses but got " + proposalConsistencySets.size() + ", this" +
                                "could be induced by nondeterministic chaincode");
                    }
                }

                if (Configuration.SEND_TO_ORDERER_DESPITE_READ) {
                    //Utils.getDiscoveredOrderers(channel);

                    CompletableFuture<BlockEvent.TransactionEvent> transactionEventCompletableFuture =
                            (ordererList == null ? channel.sendTransaction(responses) :
                                    channel.sendTransaction(responses
                                            , ordererList));

                    if (ordererList != null) {
                        for (final Orderer orderer : ordererList) {
                            readStatisticObject.getParticipatingServers().add(
                                    orderer.getName() + "|OrdererPeerCustom"
                            );
                        }
                    }

                    if (Configuration.SEND_TO_ORDERER_DESPITE_READ_AND_GET) {
                        try {
                            transactionEventCompletableFuture.get(Configuration.TIMEOUT_TRANSACTION, Configuration.TIMEOUT_UNIT_TRANSACTION);
                        } catch (TimeoutException ex) {
                            ExceptionHandler.logException(ex);
                            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                            return new ImmutablePair<>(true, "TIMEOUT_EX");
                        }
                        addOrdererToStatistics(readStatisticObject, transactionEventCompletableFuture);
                    }
                }
                readStatisticObject.setEndTime(System.nanoTime());
                return ImmutablePair.of(false, "");
            } else {
                readStatisticObject.setEndTime(-1);
                return ImmutablePair.of(false, "");
            }
        } catch (InvalidArgumentException | ExecutionException | InterruptedException | ProposalException | ServiceDiscoveryException ex) {
            ExceptionHandler.logException(ex);
            readStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }

    @Suspendable
    private void addOrdererToStatistics(final ReadStatisticObject readStatisticObject,
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
                    readStatisticObject.getParticipatingServers().add(
                            matcherOrderer.group(1).split("/")[5] + "|OrdererPeer");
                }

            } catch (IOException ex) {
                ExceptionHandler.logException(ex);
            }
        }
    }

}
