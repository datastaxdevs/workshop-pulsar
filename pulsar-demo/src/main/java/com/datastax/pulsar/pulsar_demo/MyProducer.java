package com.datastax.pulsar.pulsar_demo;

import java.io.IOException;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClient;

public class MyProducer {

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
    
    public static class MyBean {
        String id;
        String value;
        
        public MyBean(String id, String value) {
            super();
            this.id = id;
            this.value = value;
        }
        /**
         * Getter accessor for attribute 'id'.
         *
         * @return
         *       current value of 'id'
         */
        public String getId() {
            return id;
        }
        /**
         * Setter accessor for attribute 'id'.
         * @param id
         * 		new value for 'id '
         */
        public void setId(String id) {
            this.id = id;
        }
        /**
         * Getter accessor for attribute 'value'.
         *
         * @return
         *       current value of 'value'
         */
        public String getValue() {
            return value;
        }
        /**
         * Setter accessor for attribute 'value'.
         * @param value
         * 		new value for 'value '
         */
        public void setValue(String value) {
            this.value = value;
        }
        
    }
    
    public static void main(String[] args) 
    throws IOException {
        

     // Create client object
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVICE_URL)
                .authentication(AuthenticationFactory.token(AUTHENTICATION_TOKEN))
                .build();

        // Create producer on a topic
        Producer<MyBean> producer = client.newProducer(Schema.JSON(MyBean.class))
                .topic("persistent://" + TENANT_NAME + "/" + DATACENTER + "/" + TOPIV_NAME)
                .create();
         try {
             int nbmessages = 100;
             int i= 0;
                
             while( i < nbmessages) {
                 MyBean value = new MyBean("msg_" +i, "valu");
                 producer.send(value);
                 System.out.println("Message " + i + "/" + nbmessages + " sent.");
                 i++;
                 Thread.sleep(1000);
             }
         } catch (InterruptedException e) {} 
         
         // Close the producer
         producer.close();

         // Close the client
         client.close();
         System.out.println("DONE");
    }

}
