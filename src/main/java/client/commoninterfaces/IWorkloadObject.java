package client.commoninterfaces;

import java.util.Queue;

public interface IWorkloadObject {

    Queue<IListenerDisconnectionLogic> getIListenerDisconnectionLogicList();

}
