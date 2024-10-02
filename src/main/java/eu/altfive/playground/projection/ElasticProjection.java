package eu.altfive.playground.projection;

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
import org.axonframework.eventhandling.EventMessage;
import org.axonframework.extensions.kafka.eventhandling.DefaultKafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

  public ElasticProjection(ElasticModelNestedRepository repository,
      ElasticNestedEventHandler elasticNestedEventHandler) {
    this.repository = repository;
    this.elasticNestedEventHandler = elasticNestedEventHandler;
  }

  @KafkaListener(
      topics = "cc-local-event",
      groupId = "cc-local",
      batch = "true"
  )
  public void onEvent(@Payload(required = false) List<byte[]> messages,
      @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) List<String> messageKeys,
      @Header(KafkaHeaders.BATCH_CONVERTED_HEADERS) List<Map<String, Object>> headers) throws Exception {
    if (messages.isEmpty()){
      LOGGER.info("Messages empty");
    }

    // load all aggregates for this batch
    Set<String> documentIds = headers.stream()
        .map(map -> new String((byte[])map.get("axon-message-aggregate-id")))
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

    for (int i = 0; i < messages.size(); i++) {
      Class<?> messageType = Class.forName(new String((byte[])headers.get(i).get("axon-message-type")));
      Message message = (Message) messageType.getMethod("parseFrom", byte[].class).invoke(null, (Object)messages.get(i));
      if (message instanceof ModelCreated modelCreated){
        elasticNestedEventHandler.handle(documentsPerId, modelCreated);
      } else if (message instanceof VariableAdded variableAdded){
        elasticNestedEventHandler.handle(documentsPerId, variableAdded);
      } else if (message instanceof ParentSet parentSet){
        elasticNestedEventHandler.handle(documentsPerId, parentSet);
      }
    }

    documentsPerId.values().forEach(
        model -> model.setVersion(model.getVersion() != null ? model.getVersion() + 1 : 0)
    );
    repository.saveAll(documentsPerId.values());
  }

}
