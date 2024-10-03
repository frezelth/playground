package eu.altfive.playground;

import eu.altfive.playground.foundation.axon.ProtobufSerializer;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import org.axonframework.commandhandling.AsynchronousCommandBus;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandBusSpanFactory;
import org.axonframework.commandhandling.DuplicateCommandHandlerResolver;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.caching.Cache;
import org.axonframework.common.caching.WeakReferenceCache;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.Configurer;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.config.EventProcessingConfigurer.PooledStreamingProcessorConfiguration;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventMessageHandler;
import org.axonframework.eventhandling.ListenerInvocationErrorHandler;
import org.axonframework.eventhandling.PropagatingErrorHandler;
import org.axonframework.eventhandling.TrackingEventProcessorConfiguration;
import org.axonframework.extensions.kafka.KafkaProperties;
import org.axonframework.extensions.kafka.configuration.KafkaMessageSourceConfigurer;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.consumer.ConsumerFactory;
import org.axonframework.extensions.kafka.eventhandling.consumer.Fetcher;
import org.axonframework.extensions.kafka.eventhandling.consumer.streamable.StreamableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.consumer.subscribable.SubscribableKafkaMessageSource;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaEventPublisher;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.json.JacksonSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AxonConfig {

  @Bean
  public AsynchronousCommandBus commandBus(
      TransactionManager txManager, org.axonframework.config.Configuration axonConfiguration,
      DuplicateCommandHandlerResolver duplicateCommandHandlerResolver) {
    Executor executor = Executors.newFixedThreadPool(20);
    return
        AsynchronousCommandBus.builder()
            .transactionManager(txManager)
            .duplicateCommandHandlerResolver(duplicateCommandHandlerResolver)
            .spanFactory(axonConfiguration.getComponent(CommandBusSpanFactory.class))
            .messageMonitor(axonConfiguration.messageMonitor(CommandBus.class, "commandBus"))
            .executor(executor)
            .build();
//    SimpleCommandBus commandBus =
//        SimpleCommandBus.builder()
//            .transactionManager(txManager)
//            .duplicateCommandHandlerResolver(duplicateCommandHandlerResolver)
//            .messageMonitor(axonConfiguration.messageMonitor(CommandBus.class, "commandBus"))
//            .build();
//    commandBus.registerHandlerInterceptor(
//        new CorrelationDataInterceptor<>(axonConfiguration.correlationDataProviders())
//    );
  }

  @Bean
  public Cache axonCache(){
    return new WeakReferenceCache();
  }
//
//  @Bean
//  public SnapshotTriggerDefinition snapshotTrigger(Snapshotter snapshotter) {
//    return new EventCountSnapshotTriggerDefinition(snapshotter, 200);
//  }

//  @Bean
//  public <K, V> KafkaEventPublisher<?, ?> kafkaEventPublisher(
//      KafkaPublisher<K, V> kafkaPublisher,
//      KafkaProperties kafkaProperties,
//      EventProcessingConfigurer eventProcessingConfigurer) {
//    KafkaEventPublisher<K, V> kafkaEventPublisher = KafkaEventPublisher
//        .<K, V>builder()
//        .processingGroup("kafka-processor")
//        .kafkaPublisher(kafkaPublisher)
//        .build();
//
//    /*
//     * Register an invocation error handler which re-throws any exception.
//     * This will ensure a StreamingEventProcessor to enter the error mode which will retry, and it will ensure the
//     * SubscribingEventProcessor to bubble the exception to the caller. For more information see
//     *  https://docs.axoniq.io/reference-guide/configuring-infrastructure-components/event-processing/event-processors#error-handling
//     */
//    eventProcessingConfigurer.registerEventHandler(configuration -> kafkaEventPublisher)
//        .registerListenerInvocationErrorHandler(
//            kafkaEventPublisher.getProcessingGroup(), configuration -> PropagatingErrorHandler.instance()
//        )
//        .assignHandlerTypesMatching(
//            kafkaEventPublisher.getProcessingGroup(),
//            clazz -> clazz.isAssignableFrom(KafkaEventPublisher.class)
//        );
//
//    KafkaProperties.EventProcessorMode processorMode = kafkaProperties.getProducer().getEventProcessorMode();
//    if (processorMode == KafkaProperties.EventProcessorMode.SUBSCRIBING) {
//      eventProcessingConfigurer.registerSubscribingEventProcessor(kafkaEventPublisher.getProcessingGroup());
//    } else if (processorMode == KafkaProperties.EventProcessorMode.TRACKING) {
//      eventProcessingConfigurer.registerTrackingEventProcessor(kafkaEventPublisher.getProcessingGroup());
//    } else if (processorMode == KafkaProperties.EventProcessorMode.POOLED_STREAMING) {
//      EventProcessingConfigurer.PooledStreamingProcessorConfiguration psepConfig =
//          (config, builder) -> builder.batchSize(500)
//              .initialSegmentCount(20);
//      eventProcessingConfigurer.registerPooledStreamingEventProcessor(
//          kafkaEventPublisher.getProcessingGroup(),
//          c -> ((StreamableMessageSource)c.eventBus()),
//          psepConfig
//      );
//    } else {
//      throw new AxonConfigurationException("Unknown Event Processor Mode [" + processorMode + "] detected");
//    }
//
//    return kafkaEventPublisher;
//  }

  @Bean
  @Primary
  public Serializer serializer(){
    return JacksonSerializer.defaultSerializer();
  }

  @Bean
  public Serializer eventSerializer(){
    return new ProtobufSerializer();
  }

//  @Bean
//  public SnapshotTriggerDefinition snapshotTrigger(Snapshotter snapshotter) {
//    return new EventCountSnapshotTriggerDefinition(snapshotter, 200);
//  }
//
//  @Bean
//  public Cache axonCache(CacheManager cacheManager){
//    org.springframework.cache.Cache axonCache = cacheManager.getCache("axonCache");
//    return new WeakReferenceCache();
//  }

//  @Bean
//  public SequencingPolicy<EventMessage<?>> customSequencingPolicy() {
//    return event -> {
//      if (event instanceof DomainEventMessage) {
//        DomainEventMessage domainEvent = (DomainEventMessage) event;
//        return domainEvent.getMetaData().get("ancestor") != null ?
//            domainEvent.getMetaData().get("ancestor") : domainEvent.getAggregateIdentifier();
//      }
//      return null;
//    };
//  }
//
//  @Bean
//  public SubscribableKafkaMessageSource<String, byte[]> eventProcessor(
//      @Qualifier("eventSerializer") Serializer serializer,
//      KafkaProperties kafkaProperties,
//      ConsumerFactory<String, byte[]> consumerFactory,
//      Fetcher<String, byte[], EventMessage<?>> fetcher,
//      KafkaMessageConverter<String, byte[]> converter,
//      KafkaMessageSourceConfigurer kafkaMessageSourceConfigurer
//  ) {
//    return SubscribableKafkaMessageSource.<String, byte[]>builder()
//        .fetcher(fetcher)
//        .topics(Collections.singletonList(kafkaProperties.getDefaultTopic()))
//        .groupId("elastic-nested")
//        .consumerFactory(consumerFactory)
//        .messageConverter(converter)
//        .serializer(serializer)
//        .build();
//  }
//
//  @Bean
//  public KafkaMessageSourceConfigurer kafkaMessageSourceConfigurer() {
//    return new KafkaMessageSourceConfigurer();
//  }
//
//  @Configuration
//  public static class AxonKafkaConfiguration {
//
//    @Autowired
//    public void configureKafkaMessageSource(
//        Configurer configurer,
//        KafkaMessageSourceConfigurer kafkaMessageSourceConfigurer,
//        SubscribableKafkaMessageSource<String, byte[]> eventProcessor
//    ){
//      kafkaMessageSourceConfigurer.configureSubscribableSource(
//          configuration -> eventProcessor
//      );
//      configurer.registerModule(kafkaMessageSourceConfigurer);
//      configurer.eventProcessing().registerSubscribingEventProcessor(
//          "elastic-nested",
//          configuration ->  eventProcessor
//      );
//    }
//  }


  public static class RetryErrorHandler implements ListenerInvocationErrorHandler {

    @Override
    public void onError(@Nonnull Exception exception, @Nonnull EventMessage<?> event,
        @Nonnull EventMessageHandler eventHandler) throws Exception {
      throw exception;
    }
  }
}
