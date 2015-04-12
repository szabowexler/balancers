package com.loadbalancers.balancer.impl;

import com.googlecode.protobuf.pro.duplex.ClientRpcController;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */
public abstract class LoadBalancerImpl {
    protected List<ClientRpcController> workers = new LinkedList<>();
}
