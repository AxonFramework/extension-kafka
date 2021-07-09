package org.axonframework.extensions.kafka.eventhandling;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import org.apache.kafka.common.TopicPartition;

/**
 * JacksonSerializer handle Map that key belong TopicPartition must handle with special deserializer
 * For more information see:
 *     https://github.com/AxonFramework/extension-kafka/issues/34
 * @author leechedan
 * @since 4.0
 */
public class PartitionDeserializer extends KeyDeserializer {

    @Override
    public TopicPartition deserializeKey(
            String key,
            DeserializationContext context) {
        if (null == key || key.lastIndexOf('-') == -1){
            return null;
        }
        int i = key.lastIndexOf('-');
        return new TopicPartition(key.substring(0, i), Integer.valueOf(key.substring(i+1)));
    }
}