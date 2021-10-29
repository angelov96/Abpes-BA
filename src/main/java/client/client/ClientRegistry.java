package client.client;

import co.paralleluniverse.fibers.Suspendable;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ClientRegistry {

    private static final Queue<ClientObject> CLIENT_OBJECTS = new ConcurrentLinkedQueue<>();

    private ClientRegistry() {
    }

    public static Queue<ClientObject> getClientObjects() {
        return CLIENT_OBJECTS;
    }

    @Suspendable
    public static List<String> getClientIdList() {
        List<String> clientIdList = new ArrayList<>();
        for (final ClientObject clientObject : CLIENT_OBJECTS) {
            clientIdList.add(clientObject.getClientId());
        }
        return clientIdList;

    }

    @Suspendable
    public static Queue<ClientObject> getClientObjects(final List<Integer> clientIds) {
        Queue<ClientObject> clientObjects = new ConcurrentLinkedQueue<>();
        for (final ClientObject clientObject : CLIENT_OBJECTS) {
            if (clientIds.contains(clientObject.getClientNumber())) {
                clientObjects.add(clientObject);
            }
        }
        return clientObjects;
    }

}
