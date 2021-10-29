package client.client;

public class ClientObject {
    private final String CLIENT_NAME;
    private final int CLIENT_NUMBER;
    private final String CLIENT_ID;
    private final long NUMBER_OF_WRITE_OPERATIONS;
    private final long NUMBER_OF_READ_OPERATIONS;
    private final ClientRole CLIENT_ROLE;

    private ClientObject() {
        NUMBER_OF_READ_OPERATIONS = 0;
        NUMBER_OF_WRITE_OPERATIONS = 0;
        CLIENT_ID = "";
        CLIENT_NUMBER = 0;
        CLIENT_NAME = "";
        CLIENT_ROLE = ClientRole.GENERAL;
    }

    public ClientObject(final String clientNameConstructor, final int clientNumberConstructor,
                        final int numberOfWriteOperations, final int numberOfReadOperationsConstructor,
                        final ClientRole clientRoleConstructor) {
        CLIENT_NAME = clientNameConstructor;
        CLIENT_NUMBER = clientNumberConstructor;
        CLIENT_ID = getClientName() + getClientNumber();
        NUMBER_OF_WRITE_OPERATIONS = numberOfWriteOperations;
        NUMBER_OF_READ_OPERATIONS = numberOfReadOperationsConstructor;
        CLIENT_ROLE = clientRoleConstructor;
    }

    public String getClientName() {
        return CLIENT_NAME;
    }

    public int getClientNumber() {
        return CLIENT_NUMBER;
    }

    public ClientRole getClientRole() {
        return CLIENT_ROLE;
    }

    public String getClientId() {
        return CLIENT_ID;
    }

    public long getNumberOfWriteOperations() {
        return NUMBER_OF_WRITE_OPERATIONS;
    }

    public long getNumberOfReadOperations() {
        return NUMBER_OF_READ_OPERATIONS;
    }

    @Override
    public String toString() {
        return "ClientObject{" +
                "CLIENT_NAME='" + CLIENT_NAME + '\'' +
                ", CLIENT_NUMBER=" + CLIENT_NUMBER +
                ", CLIENT_ID='" + CLIENT_ID + '\'' +
                ", NUMBER_OF_WRITE_OPERATIONS=" + NUMBER_OF_WRITE_OPERATIONS +
                ", NUMBER_OF_READ_OPERATIONS=" + NUMBER_OF_READ_OPERATIONS +
                ", CLIENT_ROLE " + CLIENT_ROLE +
                '}';
    }
}
