package com.loadbalancers.rpc.server;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */
public class ServerRpcConnectionEventListener implements RpcConnectionEventListener {
    private final static Logger log = LogManager.getLogger(ServerRpcConnectionEventListener.class);
    @Override
    public void connectionReestablished(RpcClientChannel clientChannel) {
        log.info("server connectionReestablished " + clientChannel);
    }

    @Override
    public void connectionOpened(RpcClientChannel clientChannel) {
        log.info("server connectionOpened " + clientChannel);
    }

    @Override
    public void connectionLost(RpcClientChannel clientChannel) {
        log.info("server connectionLost " + clientChannel);
    }

    @Override
    public void connectionChanged(RpcClientChannel clientChannel) {
        log.info("server connectionChanged " + clientChannel);
    }
}
