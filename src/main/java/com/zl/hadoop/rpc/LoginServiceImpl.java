package com.zl.hadoop.rpc;

/**
 * Created by jacky on 2016/9/25.
 */
public class LoginServiceImpl implements LoginServiceInterface{
    @Override
    public String login(String username, String password) {
        return username + ","+ password;
    }
}
