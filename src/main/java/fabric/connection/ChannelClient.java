package fabric.connection;

import org.hyperledger.fabric.sdk.Channel;

public class ChannelClient {

    private final String name;
    private final Channel channel;

    ChannelClient(final String nameConstructor, final Channel channelConstructor) {
        this.name = nameConstructor;
        this.channel = channelConstructor;
    }

    public String getName() {
        return name;
    }

    public Channel getChannel() {
        return channel;
    }
}
