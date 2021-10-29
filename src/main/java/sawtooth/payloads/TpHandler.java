package sawtooth.payloads;

import co.paralleluniverse.fibers.Suspendable;
import org.jboss.resteasy.spi.NotImplementedYetException;
import sawtooth.helper.SawtoothHelper;

import java.util.ArrayList;
import java.util.List;

public class TpHandler {

    @Suspendable
    public static List<String> getTpInputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        String sha512HashAsString;
        String preparedAddress;
        List<String> namesToReturn = new ArrayList<>();
        for (final String name : names) {
            switch (tpEnum) {
                case GeneralTpPayload:
                    sha512HashAsString = SawtoothHelper.createSha512HashAsString(name);
                    preparedAddress =
                            tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
                    namesToReturn.add(preparedAddress);
                    break;
                default:
                    throw new NotImplementedYetException("Not implemented input");
            }
        }
        return namesToReturn;
    }

    @Suspendable
    public static List<String> getTpOutputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        String sha512HashAsString;
        String preparedAddress;
        List<String> namesToReturn = new ArrayList<>();
        for (final String name : names) {
            switch (tpEnum) {
                case GeneralTpPayload:
                    sha512HashAsString = SawtoothHelper.createSha512HashAsString(name);
                    preparedAddress =
                            tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
                    namesToReturn.add(preparedAddress);
                    break;
                default:
                    throw new NotImplementedYetException("Not implemented output");
            }
        }
        return namesToReturn;
    }

    @Suspendable
    public static String getTpRead(final TpEnum tpEnum, final String tpPrefix, final String name) {
        String sha512HashAsString;
        switch (tpEnum) {
            case GeneralTpPayload:
                sha512HashAsString = SawtoothHelper.createSha512HashAsString(name);
                return tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
            default:
                throw new NotImplementedYetException("Not implemented input");
        }
    }
}
