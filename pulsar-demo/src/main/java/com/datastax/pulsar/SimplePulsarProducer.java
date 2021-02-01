package com.datastax.pulsar;

import java.util.UUID;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class SimplePulsarProducer {
    
    public static void main(String[] args) {
        
        PulsarClient pulsarClient = null;
        Producer<DemoBean> pulsarProducer = null;
                
        try {
            DemoKesqueConfiguration conf = 
                    DemoKesqueConfiguration.getInstance();
            
            // Create client object
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(conf.getServiceUrl())
                    .authentication(AuthenticationFactory.token(conf.getAuthenticationToken()))
                    .build();
            
            // Create producer on a topic
            pulsarProducer = pulsarClient
                    .newProducer(Schema.JSON(DemoBean.class))
                    .topic("persistent://" 
                                + conf.getTenantName() + "/" 
                                + conf.getNamespace() + "/" 
                                + conf.getTopicName())
                    .create();
            
            while(true) {
              String uid = UUID.randomUUID().toString();
              pulsarProducer.send(new DemoBean(uid, "Hello World !"));
              System.out.println("[PRODUCER] -  Message " + uid  + " sent");
              Thread.sleep(conf.getWaitPeriod());
            }
            
        } catch (PulsarClientException e) {
           throw new IllegalStateException("Cannot connect to pulsar", e);
        } catch (InterruptedException e) {
            System.out.println("[INFO] - Stopped request retrieved");
        } finally {
            try {
              if (null != pulsarProducer) pulsarProducer.close();
              if (null != pulsarClient)   pulsarClient.close();
            } catch (PulsarClientException e) {}
            System.out.println("[INFO] - SimplePulsarProducer has been stopped.");
        }
    }

}
