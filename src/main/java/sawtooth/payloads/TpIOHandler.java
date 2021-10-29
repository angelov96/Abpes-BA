package sawtooth.payloads;

import co.paralleluniverse.fibers.Suspendable;
import sawtooth.configuration.Configuration;
import sawtooth.helper.SawtoothHelper;

import java.util.ArrayList;
import java.util.List;

public class TpIOHandler {

    @Suspendable
    public static List<String> getTpInputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        String sha512HashAsString;
        String preparedAddress;
        List<String> namesToReturn = new ArrayList<>();

        String startKey = names.get(0);
        for (int i = 0; i < Configuration.SIZE_IO; i++) {
            String sK = String.valueOf(Integer.parseInt(startKey) + i);
            sha512HashAsString = SawtoothHelper.createSha512HashAsString(sK);
            preparedAddress =
                    tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
            namesToReturn.add(preparedAddress);
        }
        return namesToReturn;
    }

    @Suspendable
    public static List<String> getTpOutputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        String sha512HashAsString;
        String preparedAddress;
        List<String> namesToReturn = new ArrayList<>();

        String startKey = names.get(0);
        for (int i = 0; i < Configuration.SIZE_IO; i++) {
            String sK = String.valueOf(Integer.parseInt(startKey) + i);
            sha512HashAsString = SawtoothHelper.createSha512HashAsString(sK);
            preparedAddress =
                    tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
            namesToReturn.add(preparedAddress);
        }
        return namesToReturn;
    }

    @Suspendable
    public static String getTpRead(final TpEnum tpEnum, final String tpPrefix, final String name) {
        String sha512HashAsString;
        sha512HashAsString = SawtoothHelper.createSha512HashAsString(name);
        return tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
    }
}
