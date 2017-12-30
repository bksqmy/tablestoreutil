package com.mo9.microservice.tablestore.dao;

import com.alicloud.openservices.tablestore.SyncClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author wtwei .
 * @date 2017/11/15 .
 * @time 20:32 .
 */

@Configuration
public class TsClientConfig {
    
    @Value("${aliyunTS.endPoint}")
    private String endPoint;

    @Value("${aliyunTS.accessId}")
    private String assessKeyId;

    @Value("${aliyunTS.accessKey}")
    private String accessKeySecret;

    @Value("${aliyunTS.instanceName}")
    private String instanceName;
    
    @Bean
    public SyncClient getSyncClient(){
        SyncClient client = new SyncClient(endPoint, assessKeyId, accessKeySecret, instanceName);

        return client;
    }
}
