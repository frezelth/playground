package eu.altfive.playground.foundation.axon;

import com.google.protobuf.Message;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.axonframework.common.ObjectUtils;
import org.axonframework.serialization.ChainingConverter;
import org.axonframework.serialization.Converter;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.SerializedType;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;

public class ProtobufSerializer implements Serializer {

  private final Converter converter = new ChainingConverter();

  public ProtobufSerializer() {
    System.out.println("ok");
  }

  @Override
  public <T> SerializedObject<T> serialize(@Nullable Object object,
      @Nonnull Class<T> expectedRepresentation) {
    if (object instanceof Message message) {
      byte[] data = message.toByteArray();
      T serializedContent = converter.convert(data, expectedRepresentation);
      return new SimpleSerializedObject<>(serializedContent, expectedRepresentation, typeForClass(ObjectUtils.nullSafeTypeOf(object)));
    } else if (object instanceof org.axonframework.messaging.MetaData metaData){
      MetaData m = MetaData.newBuilder()
          .putAllValues((Map)metaData)
          .build();
      byte[] data = m.toByteArray();
      T serializedContent = converter.convert(data, expectedRepresentation);
      return new SerializedMetaData<>(serializedContent, expectedRepresentation);
    } else {
      throw new IllegalArgumentException("Object is not a protobuf message");
    }
  }

  @Override
  public <T> boolean canSerializeTo(@Nonnull Class<T> expectedRepresentation) {
    return expectedRepresentation.isAssignableFrom(byte[].class);
  }

  @Override
  public <S, T> T deserialize(@Nonnull SerializedObject<S> serializedObject) {
    try {
      Class<?> messageType = Class.forName(serializedObject.getType().getName());

      if (SerializedMetaData.isSerializedMetaData(serializedObject)){
        return (T)org.axonframework.messaging.MetaData.from(
            (Map)MetaData.parseFrom((byte[]) serializedObject.getData()).getValuesMap()
        );
      }

      if (Message.class.isAssignableFrom(messageType)) {
        Message message = (Message) messageType.getMethod("parseFrom", byte[].class).invoke(null, serializedObject.getData());
        return (T) message;
      } else {
        throw new IllegalArgumentException("Serialized object is not a protobuf message");
      }
    } catch (Exception e) {
      throw new RuntimeException("Error deserializing protobuf message", e);
    }
  }

  @Override
  public Class classForType(@Nonnull SerializedType type) {
    try {
      return Class.forName(type.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Class not found for type: " + type.getName(), e);
    }
  }

  @Override
  public SerializedType typeForClass(@Nullable Class type) {
    return new SimpleSerializedType(type.getName(), null);
  }

  @Override
  public Converter getConverter() {
    return converter;
  }
}
