package sawtooth.payloads;

import co.paralleluniverse.fibers.Suspendable;
import org.jboss.resteasy.spi.NotImplementedYetException;
import sawtooth.helper.SawtoothHelper;

import java.util.ArrayList;
import java.util.List;

public class TpSbHandler {

    private static final String checkingSuffix = "_checking";
    private static final String savingsSuffix = "_savings";

    @Suspendable
    public static List<String> getTpInputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names,
                                               final String function) {
        List<String> namesToReturn = new ArrayList<>();
        switch (function) {
            case "DepositChecking":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "WriteCheck":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressSavings);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "Balance":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressSavings);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "TransactSavings":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    namesToReturn.add(preparedAddressSavings);
                }
                break;
            case "SendPayment":
                for (final String name : names.subList(0, 2)) {
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressSavings);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "Amalgamate":
                for (final String name : names.subList(0, 2)) {
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressSavings);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            default:
                throw new NotImplementedYetException("Not implemented input");
        }
        return namesToReturn;
    }

    @Suspendable
    public static List<String> getTpOutputWrite(final TpEnum tpEnum, final String tpPrefix, final List<String> names,
                                                final String function) {
        List<String> namesToReturn = new ArrayList<>();
        switch (function) {
            case "CreateAccount":
                for (final String name : names.subList(0, 2)) {
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    namesToReturn.add(preparedAddressChecking);
                    namesToReturn.add(preparedAddressSavings);
                }
                break;
            case "DepositChecking":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "WriteCheck":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "TransactSavings":
                for (final String name : names.subList(0, 1)) {
                    String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                    String preparedAddressSavings =
                            tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                    namesToReturn.add(preparedAddressSavings);
                }
                break;
            case "SendPayment":
                for (final String name : names.subList(0, 2)) {
                    String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                    String preparedAddressChecking =
                            tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                    namesToReturn.add(preparedAddressChecking);
                }
                break;
            case "Amalgamate":
                int i = 0;
                for (final String name : names.subList(0, 2)) {
                    if (i == 0) {
                        String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                        String sha512HashAsStringSavings = SawtoothHelper.createSha512HashAsString(getSavings(name));
                        String preparedAddressChecking =
                                tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                        String preparedAddressSavings =
                                tpPrefix + sha512HashAsStringSavings.substring(sha512HashAsStringSavings.length() - 64);
                        namesToReturn.add(preparedAddressChecking);
                        namesToReturn.add(preparedAddressSavings);
                    } else if (i == 1) {
                        String sha512HashAsStringChecking = SawtoothHelper.createSha512HashAsString(getChecking(name));
                        String preparedAddressChecking =
                                tpPrefix + sha512HashAsStringChecking.substring(sha512HashAsStringChecking.length() - 64);
                        namesToReturn.add(preparedAddressChecking);
                    }
                    i++;
                }
                break;
            default:
                throw new NotImplementedYetException("Not implemented input");
        }
        return namesToReturn;
    }

    @Suspendable
    public static String getTpRead(final TpEnum tpEnum, final String tpPrefix, final String name) {
        String sha512HashAsString;

        sha512HashAsString = SawtoothHelper.createSha512HashAsString(name);
        return tpPrefix + sha512HashAsString.substring(sha512HashAsString.length() - 64);
    }

    @Suspendable
    private static String getSavings(final String name) {
        return name + savingsSuffix;
    }

    @Suspendable
    private static String getChecking(final String name) {
        return name + checkingSuffix;
    }

}
