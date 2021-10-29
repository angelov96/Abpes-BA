package client.statistics;

public class GeneralMeasurementPoint {

    public enum GeneralMeasurementPointStart {
        START_OF_APPLICATION,
        START_CLIENT,
        PREPARE_EXECUTE_WORKLOAD,
        EXECUTE_WORKLOAD,
    }

    public enum GeneralMeasurementPointEnd {
        LATCH_HANDLING,
        END_OF_APPLICATION,
    }

}