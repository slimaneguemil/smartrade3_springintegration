package com.mks;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.*;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;

import java.util.Map;

@SpringBootApplication
@EnableConfigurationProperties(KafkaAppProperties.class)
public class RunMe {

    @Autowired
    private KafkaAppProperties properties;

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context
                = new SpringApplicationBuilder(RunMe.class)
                .web(WebApplicationType.NONE)
                .run(args);

     //   MessageChannel toKafka = context.getBean("toKafka2", MessageChannel.class);
       // context.getBean(RunMe.class).runDemo(context);
        //context.close();
    }

    @Service
    class Broker {

        private MessageChannel toKafka;
        private SubscribableChannel fromKafka;

        public MessageChannel getToKafka() {
            return toKafka;
        }

        public SubscribableChannel getFromKafka() {
            return fromKafka;
        }


        Broker(ConfigurableApplicationContext context){
            this.toKafka = context.getBean("toKafka2", MessageChannel.class);
            this.fromKafka = context.getBean("fromKafka", SubscribableChannel.class);
        }
    }

    private void runDemo(ConfigurableApplicationContext context) {
        MessageChannel toKafka = context.getBean("toKafka2", MessageChannel.class);
        System.out.println("Sending 10 messages...");
        Map<String, Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, this.properties.getTopic());
        for (int i = 0; i < 10; i++) {
            toKafka.send(new GenericMessage<>("foo" + i, headers));
        }
        System.out.println("Sending a null message...");
        toKafka.send(new GenericMessage<>(KafkaNull.INSTANCE, headers));

        SubscribableChannel fromKafka = context.getBean("fromKafka", SubscribableChannel.class);
       fromKafka.subscribe(
               m -> {
                   System.out.println("m = " + m);
               }
       );


    }

    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory(KafkaProperties properties) {
        Map<String, Object> producerProperties = properties.buildProducerProperties();
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    @ServiceActivator(inputChannel = "toKafka2")
    @Bean
    public MessageHandler handler(KafkaTemplate<String, String> kafkaTemplate) {
        KafkaProducerMessageHandler<String, String> handler =
                new KafkaProducerMessageHandler<>(kafkaTemplate);
        handler.setMessageKeyExpression(new LiteralExpression(this.properties.getMessageKey()));
        return handler;
    }

    @Bean
    public ConsumerFactory<?, ?> kafkaConsumerFactory(KafkaProperties properties) {
        Map<String, Object> consumerProperties = properties
                .buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        return new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> container(
            ConsumerFactory<String, String> kafkaConsumerFactory) {
        return new KafkaMessageListenerContainer<>(kafkaConsumerFactory,
                new ContainerProperties(new TopicPartitionInitialOffset(this.properties.getTopic(), 0)));
    }

    @Bean
    public KafkaMessageDrivenChannelAdapter<String, String>
    adapter(KafkaMessageListenerContainer<String, String> container) {
        KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter =
                new KafkaMessageDrivenChannelAdapter<>(container);
        kafkaMessageDrivenChannelAdapter.setOutputChannel(fromKafka());
        return kafkaMessageDrivenChannelAdapter;
    }

    @Bean
    public SubscribableChannel fromKafka() {
        return new DirectChannel();
    }

    /*
     * Boot's autoconfigured KafkaAdmin will provision the topics.
     */

    @Bean
    public NewTopic topic(KafkaAppProperties properties) {
        return new NewTopic(properties.getTopic(), 1, (short) 1);
    }

    @Bean
    public NewTopic newTopic(KafkaAppProperties properties) {
        return new NewTopic(properties.getNewTopic(), 1, (short) 1);
    }

    @Autowired
    private IntegrationFlowContext flowContext;

    @Autowired
    private KafkaProperties kafkaProperties;

    public void addAnotherListenerForTopics(String... topics) {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        // change the group id so we don't revoke the other partitions.
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) + "x");
        IntegrationFlow flow =
                IntegrationFlows
                        .from(Kafka.messageDrivenChannelAdapter(
                                new DefaultKafkaConsumerFactory<String, String>(consumerProperties), topics))
                        .channel("fromKafka")
                        .get();
        this.flowContext.registration(flow).register();
    }


}
