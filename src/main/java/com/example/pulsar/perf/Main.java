package com.example.pulsar.perf;

import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.text.MessageFormat;

@SpringBootApplication
public class Main {
    static Logger logger = LoggerFactory.getLogger(Main.class);

    PulsarClient pulsarClient;
    Consumer<String> pulsarConsumer;

    @Value("${PULSAR.SERVERS}")
    String pulsarServers;
    public int messagesReceivedLastSecond = 0;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    public void initClient() throws PulsarClientException {
        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsarServers)
                .build();
    }

    private static void updatePerfMessage(String s, Object... args) {
        PerfStates.perfMessage = MessageFormat.format(s, args);
        logger.info(PerfStates.perfMessage);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startConsuming() throws PulsarClientException {
        initClient();
        try {
            // subscribe consumer to our topic(s)
            // poll for new data
            long stime = System.currentTimeMillis();
            int count = 0;
            String currentTopic = null;
            while (true) {
                if( currentTopic == null || !currentTopic.equals(PerfStates.topic) ) {
                    if( currentTopic != null && !currentTopic.isBlank() && pulsarConsumer != null ) {
                        pulsarConsumer.close();
                    }
                    stime = System.currentTimeMillis();
                    count = 0;
                    currentTopic = PerfStates.topic;
                    if( currentTopic != null && !currentTopic.isBlank() ) {
                        pulsarConsumer = pulsarClient.newConsumer(Schema.STRING)
                                .topic(PerfStates.topic)
                                .subscriptionName("my-subscription")
                                .subscriptionType(SubscriptionType.Shared)
                                .subscribe();
                        updatePerfMessage("Pulsar consumer subscribed to {0}", PerfStates.topic);
                    }
                }

                if( currentTopic == null || currentTopic.isBlank() ) {
                    try {
                        updatePerfMessage("Waiting for topic assignment");
                        Thread.sleep(1000);
                        continue;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                Message<String> record = pulsarConsumer.receive();
                pulsarConsumer.acknowledgeAsync(record);
                long ftime = System.currentTimeMillis();
                count++;
                if( ftime - stime > 1000L ) {
                    this.messagesReceivedLastSecond = count;
                    count = 0;
                    stime = ftime;
                    updatePerfMessage("messagesReceivedLastSecond: {0}", this.messagesReceivedLastSecond);
                }
            }
         } finally {
            if( pulsarConsumer != null ) pulsarConsumer.close();
            if( pulsarClient != null ) pulsarClient.close();
            logger.info("The consumer is now gracefully closed");
        }
    }

}
