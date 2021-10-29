package graphene.listener;

import client.commoninterfaces.IListenerDisconnectionLogic;
import client.statistics.IStatistics;
import co.paralleluniverse.fibers.Suspendable;
import cy.agorise.graphenej.interfaces.WitnessResponseListener;
import cy.agorise.graphenej.models.BaseResponse;
import cy.agorise.graphenej.models.WitnessResponse;
import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GrapheneWitnessListener implements IListenerDisconnectionLogic {

    private static final Logger LOG = Logger.getLogger(GrapheneWitnessListener.class);

    @Suspendable
    public WitnessResponseListener registerWitnessResponseListener() {
        return new WitnessResponseListener() {
            @Override
            @Suspendable
            public void onSuccess(final WitnessResponse witnessResponse) {
                LOG.info("Success: " + witnessResponse.result + " Id: " + witnessResponse.id);
            }

            @Override
            @Suspendable
            public void onError(final BaseResponse.Error error) {
                LOG.error("Error: " + error.message + " Error code: " + error.code);
            }
        };
    }

    @Suspendable
    @Override
    public CompletableFuture<Boolean> isDone() {
        CompletableFuture<Boolean> done = new CompletableFuture<>();
        done.complete(true);
        return done;
    }

    @SafeVarargs
    @Override
    public final synchronized <E> Queue<IStatistics> getStatistics(final E... params) {
        return new ConcurrentLinkedQueue<>();
    }

    @Override
    @Suspendable
    public void setStatisticsAfterTimeout() {

    }
}
