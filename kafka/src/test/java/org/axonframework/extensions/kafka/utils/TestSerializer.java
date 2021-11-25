/*
 * Copyright (c) 2010-2021. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.extensions.kafka.utils;

import com.thoughtworks.xstream.XStream;
import org.axonframework.serialization.JavaSerializer;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;
import org.axonframework.serialization.json.JacksonSerializer;
import org.axonframework.serialization.xml.CompactDriver;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.junit.*;

import java.util.Base64;
import java.util.Collection;
import java.util.EnumSet;

/**
 * Enumeration of serializers for testing purposes.
 *
 * @author JohT
 */
@Ignore
public enum TestSerializer {

    JAVA {
        @SuppressWarnings("deprecation")
        private final Serializer serializer = JavaSerializer.builder().build();

        @Override
        public Serializer getSerializer() {
            return serializer;
        }

        @Override
        public String serialize(Object object) {
            return Base64.getEncoder().encodeToString(getSerializer().serialize(object, byte[].class).getData());
        }

        @Override
        public <T> T deserialize(String serialized, Class<T> type) {
            return getSerializer().deserialize(asSerializedData(Base64.getDecoder().decode(serialized), type));
        }
    },
    XSTREAM {
        private final Serializer serializer = createSerializer();

        private XStreamSerializer createSerializer() {
            XStream xStream = new XStream(new CompactDriver());
            xStream.allowTypesByWildcard(new String[]{"org.apache.kafka.**"});
            return XStreamSerializer.builder()
                                    .xStream(xStream)
                                    .classLoader(this.getClass().getClassLoader())
                                    .build();
        }

        @Override
        public Serializer getSerializer() {
            return serializer;
        }
    },
    JACKSON {
        private final Serializer serializer = JacksonSerializer.builder().build();

        @Override
        public Serializer getSerializer() {
            return serializer;
        }
    };

    public String serialize(Object object) {
        return new String(getSerializer().serialize(object, byte[].class).getData());
    }

    public <T> T deserialize(String serialized, Class<T> type) {
        return getSerializer().deserialize(asSerializedData(serialized.getBytes(), type));
    }

    public abstract Serializer getSerializer();

    public static Collection<TestSerializer> all() {
        return EnumSet.allOf(TestSerializer.class);
    }

    static <T> SerializedObject<byte[]> asSerializedData(byte[] serialized, Class<T> type) {
        SimpleSerializedType serializedType = new SimpleSerializedType(type.getName(), null);
        return new SimpleSerializedObject<>(serialized, byte[].class, serializedType);
    }
}
