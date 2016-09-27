package com.zl.hadoop.rpc;

/**
 * Created by jacky on 2016/9/25.
 */
public interface LoginServiceInterface {

    public final long versionID = 1L;

    public String login(String username, String password);
}
