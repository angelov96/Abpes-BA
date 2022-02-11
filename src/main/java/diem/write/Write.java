package diem.write;

import com.diem.DiemClient;
import graphene.statistics.WriteStatisticObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class Write  implements IWritingMethod{
    @Override
    public <E> ImmutablePair<Boolean, String> write(E... params) {
        DiemClient diemClient = (DiemClient) params[0];
        WriteStatisticObject writeStatisticObject = (WriteStatisticObject) params[3];
    }
}
