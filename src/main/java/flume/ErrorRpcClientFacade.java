package flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

import java.nio.charset.Charset;

public class ErrorRpcClientFacade {

    private RpcClient client;
    private String agentHost;
    private int agentPort;

    public ErrorRpcClientFacade(String agentHost, int agentPort) {
        this.agentHost = agentHost;
        this.agentPort = agentPort;
        this.client = RpcClientFactory.getDefaultInstance(agentHost, agentPort);
    }

    public void sendDataToFlume(String data) {
        Event event = EventBuilder.withBody(data, Charset.forName("UTF-8"));

        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            client.close();
            client = null;
            client = RpcClientFactory.getDefaultInstance(agentHost, agentPort);
        }
    }

}

