package corda.listener;

import net.corda.core.messaging.FlowProgressHandle;

public class ListenObject {

    public FlowProgressHandle<?> getFlowProgressHandle() {
        return flowProgressHandle;
    }

    public void setFlowProgressHandle(final FlowProgressHandle<?> flowProgressHandle) {
        this.flowProgressHandle = flowProgressHandle;
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    private FlowProgressHandle<?> flowProgressHandle;
    private String id;

    @Override
    public String toString() {
        return "ListenObject{" +
                "flowProgressHandle=" + flowProgressHandle.getId().getUuid() +
                ", id='" + id + '\'' +
                '}';
    }
}
