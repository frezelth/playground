package eu.altfive.playground;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;
import org.axonframework.commandhandling.AsynchronousCommandBus;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandBusSpanFactory;
import org.axonframework.commandhandling.DuplicateCommandHandlerResolver;
import org.axonframework.commandhandling.SimpleCommandBus;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.EventMessageHandler;
import org.axonframework.eventhandling.ListenerInvocationErrorHandler;
import org.axonframework.eventhandling.async.FullConcurrencyPolicy;
import org.axonframework.eventhandling.async.SequencingPolicy;
import org.axonframework.messaging.interceptors.CorrelationDataInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AxonConfig {

  @Bean
  public SimpleCommandBus commandBus(
      TransactionManager txManager, org.axonframework.config.Configuration axonConfiguration,
      DuplicateCommandHandlerResolver duplicateCommandHandlerResolver) {
//    Executor executor = Executors.newFixedThreadPool(30);
//    AsynchronousCommandBus commandBus =
//        AsynchronousCommandBus.builder()
//            .transactionManager(txManager)
//            .duplicateCommandHandlerResolver(duplicateCommandHandlerResolver)
//            .spanFactory(axonConfiguration.getComponent(CommandBusSpanFactory.class))
//            .messageMonitor(axonConfiguration.messageMonitor(CommandBus.class, "commandBus"))
//            .executor(executor)
//            .build();
    SimpleCommandBus commandBus =
        SimpleCommandBus.builder()
            .transactionManager(txManager)
            .duplicateCommandHandlerResolver(duplicateCommandHandlerResolver)
            .messageMonitor(axonConfiguration.messageMonitor(CommandBus.class, "commandBus"))
            .build();
    commandBus.registerHandlerInterceptor(
        new CorrelationDataInterceptor<>(axonConfiguration.correlationDataProviders())
    );
    return commandBus;
  }





  @Bean
  public SequencingPolicy<EventMessage<?>> customSequencingPolicy() {
    return event -> {
      if (event instanceof DomainEventMessage) {
        DomainEventMessage domainEvent = (DomainEventMessage) event;
        return domainEvent.getMetaData().get("ancestor") != null ?
            domainEvent.getMetaData().get("ancestor") : domainEvent.getAggregateIdentifier();
      }
      return null;
    };
  }

  @Bean
  public ConfigurerModule processingGroupErrorHandlingConfigurerModule() {
    return configurer -> configurer.eventProcessing(
        processingConfigurer -> processingConfigurer.registerDefaultListenerInvocationErrorHandler(
                conf -> new RetryErrorHandler()
            )
    );
  }


  public static class RetryErrorHandler implements ListenerInvocationErrorHandler {

    @Override
    public void onError(@Nonnull Exception exception, @Nonnull EventMessage<?> event,
        @Nonnull EventMessageHandler eventHandler) throws Exception {
      throw exception;
    }
  }
}
