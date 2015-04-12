package com.loadbalancers.rpc.client;

import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
* @author Elias Szabo-Wexler
* @since 09/April/2015
*/
class ClientRpcConnectionEventListener implements RpcConnectionEventListener {

    private final static Logger log = LogManager.getLogger(ClientRpcConnectionEventListener.class);

    @Override
    public void connectionReestablished(RpcClientChannel clientChannel) {
        log.info("client connectionReestablished " + clientChannel);
    }

    @Override
    public void connectionOpened(RpcClientChannel clientChannel) {
        log.info("client connectionOpened " + clientChannel);
    }

    @Override
    public void connectionLost(RpcClientChannel clientChannel) {
        log.info("client connectionLost " + clientChannel);
    }

    @Override
    public void connectionChanged(RpcClientChannel clientChannel) {
        log.info("client connectionChanged " + clientChannel);
    }
}
