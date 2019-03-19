package com.mks;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.GenericMessage;

import java.util.Collections;
import java.util.Map;

@SpringBootApplication
public class RunLfx2 {
    @Autowired
    RunMe.Broker broker;

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context
                = new SpringApplicationBuilder(RunLfx2.class)
                .web(WebApplicationType.NONE)
                .run(args);
        context.getBean(RunLfx2.class).runDemo(context);
        //context.close();
    }

    private void runDemo(ConfigurableApplicationContext context) {

        SubscribableChannel fromKafka = broker.getFromKafka();
        fromKafka.subscribe(
                m -> {
                    System.out.println("m = " + m);
                }
        );

        MessageChannel toKafka = broker.getToKafka();
        System.out.println("Sending 10 messages...");
        Map<String, Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, "si.topic");
        for (int i = 0; i < 10; i++) {
            toKafka.send(new GenericMessage<>("foo" + i, headers));
        }
        System.out.println("Sending a null message...");
        toKafka.send(new GenericMessage<>(KafkaNull.INSTANCE, headers));

    }

}
