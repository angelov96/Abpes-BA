package sawtooth.payloads;

import co.paralleluniverse.fibers.Suspendable;

import java.util.Collections;
import java.util.List;

public class TpNoAddressHandler {

    @Suspendable
    public static List<String> getTpInputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        return Collections.EMPTY_LIST;
    }

    @Suspendable
    public static List<String> getTpOutputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names) {
        return Collections.EMPTY_LIST;
    }

    @Suspendable
    public static String getTpRead(final TpEnum tpEnum, final String tpPrefix, final String name) {
        return "";
    }
}
