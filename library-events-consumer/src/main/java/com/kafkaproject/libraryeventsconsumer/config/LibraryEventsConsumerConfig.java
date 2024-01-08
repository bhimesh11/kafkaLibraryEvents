package com.kafkaproject.libraryeventsconsumer.config;

import com.kafkaproject.libraryeventsconsumer.GenericConstants.genericConstants;
import com.kafkaproject.libraryeventsconsumer.service.failureService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

import static com.kafkaproject.libraryeventsconsumer.GenericConstants.genericConstants.DEAD;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig
{
    public final KafkaProperties properties;



    @Autowired
    KafkaTemplate kafkaTemplate;

    @Value("${topics.retry:library-events.RETRY}")
    private String retryTopic;

    @Value("${topics.dlt:library-events.DLT}")
    private String deadLetterTopic;

    @Autowired
    failureService failureService;

    @Autowired
    genericConstants values;

    public LibraryEventsConsumerConfig(KafkaProperties properties)
    {
        this.properties = properties;
    }


    public DefaultErrorHandler errorHandler()
    {
      var exceptionToIgnoreList =  List.of(IllegalArgumentException.class);
      var fixedBackOff = new FixedBackOff(10,2);

        ExponentialBackOffWithMaxRetries expBackOff = new ExponentialBackOffWithMaxRetries(2);
        expBackOff.setInitialInterval(1000L);
        expBackOff.setMultiplier(2.0);
        expBackOff.setMaxInterval(2000L);

var defaultErrorHandler =   new DefaultErrorHandler( consumerRecordRecoverer, // publishingRecoverer(),
                fixedBackOff);

        exceptionToIgnoreList.forEach(defaultErrorHandler::addNotRetryableExceptions);
        defaultErrorHandler.setRetryListeners((record,
                                               ex,
                                               deliveryAttempt) ->
                log.info("Failed Record in Retry Listener  exception : {} , deliveryAttempt : {} ", ex.getMessage(), deliveryAttempt)
                );
        return defaultErrorHandler;
    }

    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
       var consume = (ConsumerRecord<Integer,String>)record;

        if (exception.getCause() instanceof RecoverableDataAccessException) {
            log.info("Inside the recoverable logic");
            failureService.saveFailObject(consume,exception,values.RETRY);
        } else {
            log.info("Inside the non recoverable logic and skipping the record : {}", record);
            failureService.saveFailObject(consume,exception,values.DEAD);
        }
    };


    private DeadLetterPublishingRecoverer publishingRecoverer() {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,(r, e) ->
        {
            log.error("Exception in publishingRecoverer : {} \", e.getMessage(), e");
            if(e.getCause() instanceof RecoverableDataAccessException)
            {
                return new TopicPartition(retryTopic,r.partition());
            }
            else {
                return new TopicPartition(deadLetterTopic,r.partition());
            }
        });
        return recoverer;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory,
            ObjectProvider<ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>>> kafkaContainerCustomizer,
            ObjectProvider<SslBundles> sslBundles) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, kafkaConsumerFactory.getIfAvailable(() -> new DefaultKafkaConsumerFactory<>(
                this.properties.buildConsumerProperties(sslBundles.getIfAvailable()))));
        //factory.setConcurrency(3);
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
