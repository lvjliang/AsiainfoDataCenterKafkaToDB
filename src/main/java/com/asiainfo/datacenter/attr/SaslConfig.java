package com.asiainfo.datacenter.attr;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: kafkaclientdemo
 * @description:
 * @author: 董建斌
 * @create: on 2018/9/26.
 **/
public class SaslConfig extends Configuration {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        String appKey = "97-xinxihua-01";    //登陆认证用户名
        String secretKey = "DZ4acLWYz"; //登陆认证密码
        Map<String, String> options = new HashMap<String, String>();
        options.put("username", appKey);
        options.put("password", secretKey);
        AppConfigurationEntry entry = new AppConfigurationEntry(
                "org.apache.kafka.common.security.plain.PlainLoginModule",
                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
        AppConfigurationEntry[] configurationEntries = new AppConfigurationEntry[1];
        configurationEntries[0] = entry;
        return configurationEntries;
    }
}