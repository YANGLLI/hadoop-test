package com.zl.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by h on 2016/9/25.
 */
public class LoginController {

    public static void main(String[] args) throws IOException {

        LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("172.16.16.25", 10000), new Configuration());

        System.out.println(proxy.login("jacky","123"));
    }
}
