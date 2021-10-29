package fabric.statistics;

public class CustomStatisticObject<E1> extends client.statistics.CustomStatisticObject<E1> {

    private String id;
    private String sharedId;
    private E1 value;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Override
    public String getSharedId() {
        return sharedId;
    }

    @Override
    public void setSharedId(final String sharedId) {
        this.sharedId = sharedId;
    }

    @Override
    public String toString() {
        return "CustomStatisticObject{" +
                "id='" + id + '\'' +
                ", sharedId='" + sharedId + '\'' +
                ", value=" + value +
                '}';
    }

    @Override
    public E1 getValue() {
        return value;
    }

    @Override
    public void setValue(final Object value) {
        this.value = (E1) value;
    }

}
