package com.datastax.pulsar;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class DemoKesqueConfiguration {
    
    private static DemoKesqueConfiguration _instance;
    
    private String serviceUrl;
    
    private String tenantName;
    
    private String namespace;
    
    private String topicName;
    
    private String authenticationToken;
    
    private int waitPeriod = 1000;
    
    public static synchronized DemoKesqueConfiguration getInstance() {
        if (null == _instance) {
            _instance = new DemoKesqueConfiguration();
        }
        return _instance;
    }
    
    private DemoKesqueConfiguration() {
        try {
            Properties properties = new Properties();
            properties.load(Thread
                    .currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream("application.properties"));
            
            this.serviceUrl = properties.getProperty("kesque.service_url");
            if (null == serviceUrl) {
                throw new IllegalArgumentException("Cannot read serviceUrl in conf file");
            }
            
            this.namespace  = properties.getProperty("kesque.namespace");
            if (null == namespace) {
                throw new IllegalArgumentException("Cannot read namespace in conf file");
            }
           
            this.tenantName  = properties.getProperty("kesque.tenant_name");
            if (null == tenantName) {
                throw new IllegalArgumentException("Cannot read tenant_name in conf file");
            }
            
            this.topicName  = properties.getProperty("kesque.topic_name");
            if (null == topicName) {
                throw new IllegalArgumentException("Cannot read topic_name in conf file");
            }
            
            this.authenticationToken  = properties.getProperty("kesque.authentication_token");
            if (null == authenticationToken) {
                throw new IllegalArgumentException("Cannot read authenticationo_token in conf file");
            }
            
            String newPeriod = properties.getProperty("demo.wait_between_message");
            if (null != newPeriod) {
                waitPeriod = Integer.parseInt(newPeriod);
            }
            
            System.out.println("[INFO] - Configuration has been loaded successfully");
           
        } catch (FileNotFoundException e) {
            throw new IllegalStateException(e);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    
    public static void main(String[] args) {
        new DemoKesqueConfiguration();
    }


    /**
     * Getter accessor for attribute 'serviceUrl'.
     *
     * @return
     *       current value of 'serviceUrl'
     */
    public String getServiceUrl() {
        return serviceUrl;
    }

    /**
     * Getter accessor for attribute 'tenantName'.
     *
     * @return
     *       current value of 'tenantName'
     */
    public String getTenantName() {
        return tenantName;
    }


    /**
     * Getter accessor for attribute 'namespace'.
     *
     * @return
     *       current value of 'namespace'
     */
    public String getNamespace() {
        return namespace;
    }


    /**
     * Getter accessor for attribute 'topicName'.
     *
     * @return
     *       current value of 'topicName'
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * Getter accessor for attribute 'authenticationToken'.
     *
     * @return
     *       current value of 'authenticationToken'
     */
    public String getAuthenticationToken() {
        return authenticationToken;
    }

    /**
     * Getter accessor for attribute 'waitPeriod'.
     *
     * @return
     *       current value of 'waitPeriod'
     */
    public int getWaitPeriod() {
        return waitPeriod;
    }
    

}
