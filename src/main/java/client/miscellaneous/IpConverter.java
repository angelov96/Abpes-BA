package client.miscellaneous;

import client.supplements.ExceptionHandler;
import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpConverter {

    private static final Logger LOG = Logger.getLogger(IpConverter.class);

    public static String hostToIp(final String host) {
        try {
            InetAddress inetAddress = InetAddress.getByName(host);
            LOG.info("IpAddress is: " + inetAddress.getHostAddress());
            return inetAddress.getHostAddress();
        } catch (UnknownHostException ex) {
            ExceptionHandler.logException(ex);
        }
        return host;
    }

    public static String ipToHost(final String ip) {
        try {
            InetAddress addr = InetAddress.getByName(ip);
            String host = addr.getHostName();
            LOG.info("HostAddress is: " + host);
            return host;
        } catch (UnknownHostException ex) {
            ExceptionHandler.logException(ex);
        }
        return ip;
    }

}
