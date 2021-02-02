package com.datastax.pulsar;

import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

public class SimplePulsarConsumer {

 public static void main(String[] args) {
        
        PulsarClient pulsarClient = null;
        Consumer<DemoBean> pulsarConsumer = null;
                
        try {
            DemoKesqueConfiguration conf = 
                    DemoKesqueConfiguration.getInstance();
            
            // Create client object
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(conf.getServiceUrl())
                    .authentication(AuthenticationFactory.token(conf.getAuthenticationToken()))
                    .build();
            
            pulsarConsumer = pulsarClient.newConsumer(Schema.JSON(DemoBean.class))
                    .topic("persistent://" 
                            + conf.getTenantName() + "/" 
                            + conf.getNamespace() + "/" 
                            + conf.getTopicName())
                    .startMessageIdInclusive()
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscriptionName("SimplePulsarConsumer")
                    .subscribe();
            
            while(true) {
               Message<DemoBean> msg = pulsarConsumer.receive(conf.getWaitPeriod(), TimeUnit.MILLISECONDS);
                if(msg != null){
                    System.out.println("[CONSUMER] - Message received:" + new String(msg.getData()));
                    pulsarConsumer.acknowledge(msg);
                    Thread.sleep(conf.getWaitPeriod());
                }
            }
            
        } catch (PulsarClientException e) {
           throw new IllegalStateException("Cannot connect to pulsar", e);
        } catch (InterruptedException e) {
            System.out.println("[INFO] - Stopped request retrieved");
        } finally {
            try {
              if (null != pulsarConsumer) pulsarConsumer.close();
              if (null != pulsarClient)   pulsarClient.close();
            } catch (PulsarClientException e) {}
            System.out.println("[INFO] - SimplePulsarProducer has been stopped.");
        }
    }
 
}
