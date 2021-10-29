package graphene.payload_patterns;

import client.client.ClientObject;
import co.paralleluniverse.fibers.Suspendable;
import graphene.components.BaseOperation;
import graphene.configuration.Configuration;
import graphene.listener.GrapheneSubscription;
import graphene.payloads.IGrapheneWritePayload;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransactionDispatcher implements IOperationToTransactionDispatcher {

    private static final Logger LOG = Logger.getLogger(TransactionDispatcher.class);

    private final Map<BaseOperation, IGrapheneWritePayload> operationMapping = new HashMap<>();

    public Map<BaseOperation, IGrapheneWritePayload> getPayloadMapping() {
        return operationMapping;
    }

    @SafeVarargs
    @Suspendable
    @Override
    public final <E1, E2> E2 dispatchOperations(final E1... params) {

        if (params.length != 2) {
            throw new NotYetImplementedException("Not yet implemented");
        }
        List<IGrapheneWritePayload> iGrapheneWritePayloads = (List<IGrapheneWritePayload>) params[0];
        ClientObject clientObject = (ClientObject) params[1];

        List<BaseOperation> operationList = new ArrayList<>();

        int operationsInTransaction = 0;

        LOG.info("Dispatching a total of: " + iGrapheneWritePayloads.size() + " payloads to a transaction with " + Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION +
                " operations");

        for (final IGrapheneWritePayload iGrapheneWritePayload : iGrapheneWritePayloads) {

            operationMapping.put(iGrapheneWritePayload.getPayload(), iGrapheneWritePayload);
            operationList.add(iGrapheneWritePayload.getPayload());

            operationsInTransaction++;

            GrapheneSubscription.getObtainedEventsMap().get(clientObject.getClientId()).get(iGrapheneWritePayload.getEventPrefix() + iGrapheneWritePayload.getSignature()).setLeft(System.nanoTime());

            if (operationsInTransaction >= Configuration.NUMBER_OF_OPERATIONS_PER_TRANSACTION) {
                LOG.info("Reached threshold of operations, splitting transaction");
                return (E2) operationList;
            }

        }

        return (E2) operationList;

    }

}