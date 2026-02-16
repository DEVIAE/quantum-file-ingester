package com.quantum.ingester.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantum.common.config.JacksonConfig;
import jakarta.jms.ConnectionFactory;
import org.apache.camel.component.jms.JmsComponent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.connection.CachingConnectionFactory;

@Configuration
public class JmsConfig {

    @Bean
    public JmsComponent jms(ConnectionFactory connectionFactory) {
        CachingConnectionFactory cachingFactory = new CachingConnectionFactory();
        cachingFactory.setTargetConnectionFactory(connectionFactory);
        cachingFactory.setSessionCacheSize(20);

        JmsComponent jmsComponent = JmsComponent.jmsComponentAutoAcknowledge(cachingFactory);
        jmsComponent.setDeliveryPersistent(true);
        jmsComponent.setConcurrentConsumers(1);
        return jmsComponent;
    }

    @Bean
    public ObjectMapper objectMapper() {
        return JacksonConfig.createObjectMapper();
    }
}
