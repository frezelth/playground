package eu.altfive.playground.projection;

import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.extractAxonMetadata;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.valueAsLong;
import static org.axonframework.extensions.kafka.eventhandling.HeaderUtils.valueAsString;
import static org.axonframework.messaging.Headers.AGGREGATE_ID;
import static org.axonframework.messaging.Headers.AGGREGATE_SEQ;
import static org.axonframework.messaging.Headers.AGGREGATE_TYPE;
import static org.axonframework.messaging.Headers.MESSAGE_ID;
import static org.axonframework.messaging.Headers.MESSAGE_REVISION;
import static org.axonframework.messaging.Headers.MESSAGE_TIMESTAMP;
import static org.axonframework.messaging.Headers.MESSAGE_TYPE;

import com.google.protobuf.Message;
import eu.altfive.playground.command.CreateModel;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.axonframework.eventhandling.EventData;
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericDomainEventEntry;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.HeaderUtils;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.messaging.MetaData;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class ElasticProjection {

  private final static Logger LOGGER = LoggerFactory.getLogger(ElasticProjection.class);
  private final ElasticModelNestedRepository repository;
  private final ElasticNestedEventHandler elasticNestedEventHandler;
  private final ApplicationContext applicationContext;
//  private final Serializer serializer;
  private final KafkaMessageConverter<String, byte[]> kafkaMessageConverter;

  public static ThreadLocal<ElasticModelNested> currentDocument = new ThreadLocal<>();

  public ElasticProjection(ElasticModelNestedRepository repository,
      ElasticNestedEventHandler elasticNestedEventHandler,
      ApplicationContext applicationContext,
      KafkaMessageConverter<String, byte[]> kafkaMessageConverter) {
    this.repository = repository;
    this.elasticNestedEventHandler = elasticNestedEventHandler;
    this.applicationContext = applicationContext;
    this.kafkaMessageConverter = kafkaMessageConverter;
  }

  @KafkaListener(
      topics = "cc-local-event",
      groupId = "cc-local",
      batch = "true"
  )
  public void onEvent(@Payload(required = false) List<ConsumerRecord<String, byte[]>> messages) throws Exception {
    if (messages.isEmpty()){
      LOGGER.info("Messages empty");
    }

    // load all aggregates for this batch
    Set<String> documentIds = messages.stream()
        .map(message -> HeaderUtils.valueAsString(message
            .headers(), AGGREGATE_ID))
        .collect(Collectors.toSet());

    Map<String, ElasticModelNested> documentsPerId = ((List<ElasticModelNested>) repository.findAllById(
        documentIds)).stream()
        .collect(Collectors.toMap(
            ElasticModelNested::getId,
            elasticModelNested -> elasticModelNested
        ));

    for (String documentId : documentIds) {
      documentsPerId.computeIfAbsent(
          documentId,
          ElasticModelNested::new);
    }

    for (ConsumerRecord<String, byte[]> message : messages) {
//      Class<?> messageType = Class.forName(new String((byte[])headers.get(i).get("axon-message-type")));
//      String documentId = new String((byte[])headers.get(i).get("axon-message-aggregate-id"));
      String aggregateId = valueAsString(message
          .headers(), AGGREGATE_ID);

      EventMessage<?> eventMessage = kafkaMessageConverter.readKafkaMessage(message)
          .orElseThrow();
      try {
        currentDocument.set(documentsPerId.get(aggregateId));
        applicationContext.publishEvent(eventMessage.getPayload());
      } finally {
        currentDocument.remove();
      }
    }

    documentsPerId.values().forEach(
        model -> model.setVersion(model.getVersion() != null ? model.getVersion() + 1 : 0)
    );
    repository.saveAll(documentsPerId.values());
  }

}
