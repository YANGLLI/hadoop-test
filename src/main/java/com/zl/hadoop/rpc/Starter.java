package com.zl.hadoop.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by jacky on 2016/9/25.
 */
public class Starter {

    public static void main(String[] args) throws IOException {

        RPC.Builder builder = new RPC.Builder(new Configuration());
        builder.setBindAddress("172.16.16.25").setPort(10000).setProtocol(LoginServiceInterface.class).setInstance(new LoginServiceImpl());

        RPC.Server server = builder.build();

        server.start();

    }
}
