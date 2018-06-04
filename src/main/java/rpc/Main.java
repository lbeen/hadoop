package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolProxy;
import org.apache.hadoop.ipc.RPC;
import org.junit.Test;
import rpc.bo.ServiceImpl;
import rpc.bo.ServiceInterface;

import java.net.InetSocketAddress;

/**
 * @author 李斌
 */
public class Main {

    public static void main(String[] args) throws Exception {
        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setProtocol(ServiceInterface.class).setInstance(new ServiceImpl());
        builder.setBindAddress("localhost").setPort(10000);
        RPC.Server server = builder.build();
        server.start();
    }

    @Test
    public void client() throws Exception {
        ProtocolProxy<ServiceInterface> proxy = RPC.getProtocolProxy(ServiceInterface.class, 1L, new InetSocketAddress("localhost", 10000), new Configuration());
        String s = proxy.getProxy().login("tom");
        System.out.println(s);
    }
}
