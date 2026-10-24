package com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Protocol {
    public static final byte PRODUCE = 0x01;

    public static ByteBuffer encodeProduceRequest(
            String topic, int partition, byte[] message) {

        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Topic 名称过长");
        }

        ByteBuffer buffer =
                ByteBuffer.allocate(11 + topicBytes.length + message.length);

        buffer.put(PRODUCE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }
}
