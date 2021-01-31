package com.datastax.pulsar.pulsar_demo;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

import com.datastax.pulsar.pulsar_demo.MyProducer.MyBean;

public class MyClient {
    
    
  private static final String SERVICE_URL = "pulsar+ssl://uswest2.aws.kafkaesque.io:6651";
    
    private static final String TENANT_NAME = "kesque-astra-demo";    
    private static final String DATACENTER  = "local-uswest2-aws";
    private static final String TOPIV_NAME  = "astra";
    
    private static final String AUTHENTICATION_TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJrZXNxdWUtYXN0"
            + "cmEtZGVtby1jbGllbnQtNjAxNWRmODViODU5OCJ9.m_FIe8kDrxUVPafsEyosLlnqllu5x7arDYeI7h9ag3GxC"
            + "RrTELI_x9eTbfZ3TPdlI_i9m7Beu0VXsH-LIt7u2JihqvOXNUPqduKxs7y4_P3k0VfYOy3r8d9NRKParbNbVJq"
            + "ES_BsfF6DFOZIu1tdRAoitVQxbfLN42LgwqM3R8IBnxpjJyGRjJA6VzPeh5QfZPtNLccqL_7Pt97tbL2YC9mYT"
            + "FagSV68zsDEWbmRApkfzbMV5tgQuJZH4QBh_jHa9xsTdWy5Adxkfv-ch_gAzhqnAOdnsY6SiimYJ2nlsiDfSL6"
            + "-3jIJasJ2QAplk8TBumpRJN6SP4cAry6WWQSYvQ";
    

    public static void main(String[] args) throws IOException
    {

        // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(AuthenticationFactory.token(AUTHENTICATION_TOKEN))
                .build();

        // Create consumer on a topic with a subscription
        System.out.println("Starting COnsumer");
        Consumer<MyBean> consumer = client.newConsumer(Schema.JSON(MyBean.class))
                .topic("persistent://" + TENANT_NAME + "/" + DATACENTER + "/" + TOPIV_NAME)
                .startMessageIdInclusive()
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("java_client")
                .subscribe();

        int limit = 100;
        int i= 0;
        do {
            Message<MyBean> msg = consumer.receive(1, TimeUnit.SECONDS);
            if(msg != null){
                System.out.println("Message received:" + new String(msg.getData()));
                consumer.acknowledge(msg);
                i++;
            }
        } while (i<limit);

        //Close the consumer
        consumer.close();

        // Close the client
        client.close();
        
        System.out.println("DONE");

    }

}
