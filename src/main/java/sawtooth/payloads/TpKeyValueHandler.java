package sawtooth.payloads;

import co.paralleluniverse.fibers.Suspendable;
import sawtooth.helper.SawtoothHelper;

import java.util.ArrayList;
import java.util.List;

public class TpKeyValueHandler {

    @Suspendable
    public static List<String> getTpInputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        String sha512HashAsString;
        String preparedAddress;
        List<String> namesToReturn = new ArrayList<>();
        String key = names.get(0);
        sha512HashAsString = SawtoothHelper.createSha512HashAsString(key);
        preparedAddress =
                tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
        namesToReturn.add(preparedAddress);
        return namesToReturn;
    }

    @Suspendable
    public static List<String> getTpOutputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        String sha512HashAsString;
        String preparedAddress;
        List<String> namesToReturn = new ArrayList<>();
        String key = names.get(0);
        sha512HashAsString = SawtoothHelper.createSha512HashAsString(key);
        preparedAddress =
                tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
        namesToReturn.add(preparedAddress);
        return namesToReturn;
    }

    @Suspendable
    public static String getTpRead(final TpEnum tpEnum, final String tpPrefix, final String name) {
        String sha512HashAsString;
        sha512HashAsString = SawtoothHelper.createSha512HashAsString(name);
        return tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
    }
}
