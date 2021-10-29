package corda.write;

import client.configuration.GeneralConfiguration;
import client.statistics.IStatistics;
import client.supplements.ExceptionHandler;
import co.paralleluniverse.fibers.Suspendable;
import corda.configuration.Configuration;
import corda.helper.Helper;
import corda.listener.CloseFlowPoint;
import corda.listener.Listen;
import corda.listener.ListenObject;
import corda.statistics.WriteStatisticObject;
import net.corda.core.flows.FlowLogic;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.messaging.FlowHandle;
import net.corda.core.messaging.FlowProgressHandle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.hibernate.cfg.NotYetImplementedException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public class Write implements IWritingMethod {

    private static final Logger LOG = Logger.getLogger(Write.class);

    @Suspendable
    @SafeVarargs
    @Override
    public final <E> ImmutablePair<Boolean, String> write(final E... params) {

        if (params.length == 7) {
            CordaRPCOps proxy = (CordaRPCOps) params[0];
            Pair<Class<FlowLogic<?>>, List<E>> pair = (Pair<Class<FlowLogic<?>>, List<E>>) params[1];
            WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[2];
            Queue<IStatistics> iStatistics = (Queue<IStatistics>) params[3];
            String id = (String) params[4];
            double threshold = (Double) params[5];
            int numberOfExpectedEvents = (Integer) params[6];

            return write(proxy, pair, writeStatisticObject, iStatistics, id, threshold, numberOfExpectedEvents);
        }
        throw new NotYetImplementedException("Not yet implemented function called");
    }

    @Suspendable
    private <E> ImmutablePair<Boolean, String> write(final CordaRPCOps proxy,
                                                     final Pair<Class<FlowLogic<?>>, List<E>> pair,
                                                     final WriteStatisticObject writeStatisticObject,
                                                     final Queue<IStatistics> iStatistics,
                                                     final String id, final double threshold,
                                                     final int numberOfExpectedEvents) {

        writeStatisticObject.setStartTime(System.nanoTime());

        List<E> objectList = pair.getValue();
        LOG.debug("ObjectList: " + Arrays.toString(Helper.unpack(objectList.toArray())));
        LOG.debug("Key: " + pair.getKey() + " Value: " + objectList);

        FlowProgressHandle<?> flowProgressHandle;
        FlowHandle<?> flowHandle;

        try {
            if (Configuration.SEND_TRACKED) {
                flowProgressHandle =
                        proxy.startTrackedFlowDynamic(pair.getKey(),
                                objectList.toArray());

                if (Configuration.ENABLE_LISTENER) {
                    ListenObject listenObject = new ListenObject();
                    listenObject.setId(id);
                    listenObject.setFlowProgressHandle(flowProgressHandle);
                    Listen.getObservableList().add(listenObject);
                }

                if (Configuration.SEND_WRITE_SYNC) {
                    try {
                        LOG.info(" ResultFlowHandler: " + flowProgressHandle.getReturnValue().get() + " UUID: " + flowProgressHandle.getId().getUuid());
                        writeStatisticObject.setEndTime(System.nanoTime());
                        if (Configuration.SET_TRANSACTION_ID) {
                            Map<String, String> obtainedTransactionIdMap = Listen.getObtainedTransactionIdMap(id);
                            if (obtainedTransactionIdMap != null) {
                                String txId =
                                        obtainedTransactionIdMap.get(flowProgressHandle.getId().getUuid().toString());
                                if (txId != null) {
                                    LOG.info("TxId: " + txId);
                                } else {
                                    LOG.error("UUID " + flowProgressHandle.getId().getUuid() + " not found in map");
                                }
                            } else {
                                LOG.error("Map is null");
                            }
                        }
                        return ImmutablePair.of(false, flowProgressHandle.getId().getUuid().toString());
                    } catch (InterruptedException | ExecutionException | IllegalStateException ex) {
                        ExceptionHandler.logException(ex);
                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        return ImmutablePair.of(true, ex.getMessage());
                    } finally {
                        if (Configuration.CLOSE_FLOW_POINT == CloseFlowPoint.WRITE ||
                                Configuration.CLOSE_FLOW_POINT == CloseFlowPoint.FIRST
                        ) {
                            flowProgressHandle.close();
                        }
                    }
                } else if (Configuration.SEND_WRITE_ASYNC) {
                /*try {
                    listen.getDone().get();
                } catch (InterruptedException | ExecutionException ex) {
                    ExceptionHandler.logException(ex);
                    writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                    return ImmutablePair.of(true, ex.getMessage());
                } finally {
                    flowProgressHandle.close();
                }

                if (Configuration.CLOSE_FLOW_POINT == CloseFlowPoint.WRITE ||
                        Configuration.CLOSE_FLOW_POINT == CloseFlowPoint.FIRST
                ) {
                    flowProgressHandle.close();
                }*/

                    LOG.debug("Sent async");
                    writeStatisticObject.setEndTime(-1);
                    return ImmutablePair.of(false, "");
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }
            } else {
                flowHandle = proxy.startFlowDynamic(pair.getKey(),
                        objectList.toArray());

                if (Configuration.SEND_WRITE_SYNC) {
                    try {
                        LOG.info(" Result: " + flowHandle.getReturnValue().get() + " UUID: " + flowHandle.getId().getUuid());
                        writeStatisticObject.setEndTime(System.nanoTime());
                        if (Configuration.SET_TRANSACTION_ID) {
                            Map<String, String> obtainedTransactionIdMap = Listen.getObtainedTransactionIdMap(id);
                            if (obtainedTransactionIdMap != null) {
                                String txId =
                                        obtainedTransactionIdMap.get(flowHandle.getId().getUuid().toString());
                                if (txId != null) {
                                    LOG.info("TxId: " + txId);
                                } else {
                                    LOG.error("UUID " + flowHandle.getReturnValue().get() + " not found in map");
                                }
                            } else {
                                LOG.error("Map is null");
                            }
                        }
                        return ImmutablePair.of(false, flowHandle.getId().getUuid().toString());
                    } catch (InterruptedException | ExecutionException | IllegalStateException ex) {
                        ExceptionHandler.logException(ex);
                        writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
                        return ImmutablePair.of(true, ex.getMessage());
                    } finally {
                        flowHandle.close();
                    }
                } else if (Configuration.SEND_WRITE_ASYNC) {
                    //flowHandle.close();
                    writeStatisticObject.setEndTime(-1);
                    LOG.debug("Sent async, connection closed");
                    return ImmutablePair.of(false, "");
                } else {
                    throw new NotYetImplementedException("Not yet implemented");
                }
            }
        } catch (/*todo specify concrete exception(s) CouldNotStartFlowException*/ Exception ex) {
            ExceptionHandler.logException(ex);
            writeStatisticObject.setEndTime(GeneralConfiguration.DEFAULT_ERROR_TIMESTAMP);
            return ImmutablePair.of(true, ex.getMessage());
        }
    }
}
