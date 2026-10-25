package com.simplekafka.broker;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;

    public static final byte ERROR_RESPONSE = 0x1F;

    public static final byte REPLICATE = 0x21;
    public static final byte TOPIC_NOTIFICATION = 0x23;

    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static class FetchResult {
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public int getMessageCount() {
            return messages.length;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(
                List<BrokerInfo> brokers,
                List<TopicMetadata> topics,
                String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message){
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

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes){
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Topic 名称过长");
        }

        if (partition < 0 || offset < 0 || maxBytes <= 0) {
            throw new IllegalArgumentException(
                    "分区和偏移量不能为负数，最大读取字节数必须大于 0");
        }

        ByteBuffer buffer = ByteBuffer.allocate(19 + topicBytes.length);

        buffer.put(FETCH);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);

        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeMetadataRequest(){
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor){
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length == 0 || topicBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Topic 名称不能为空或过长");
        }
        if (numPartitions <= 0 || replicationFactor <= 0) {
            throw new IllegalArgumentException("分区数和复制因子必须大于 0");
        }

        ByteBuffer buffer = ByteBuffer.allocate(9 + topicBytes.length);

        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);

        buffer.flip();
        return buffer;
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {   //Contains offset and error information for produce operations
        // 1. 读取响应类型
        byte responseType = buffer.get();

        // 2. 处理错误响应
        if (responseType == ERROR_RESPONSE) {
            short errorLength = buffer.getShort();
            byte[] errorBytes = new byte[errorLength];
            buffer.get(errorBytes);

            String error = new String(errorBytes, StandardCharsets.UTF_8);
            return new ProduceResult(-1L, error);
        }

        // 3. 检查是否为生产响应
        if (responseType != PRODUCE_RESPONSE) {
            return new ProduceResult(-1L, "无效的响应类型：" + responseType);
        }

        // 4. 读取消息偏移量和状态，约定状态 0 表示成功
        long offset = buffer.getLong();
        byte status = buffer.get();

        if (status != 0) {
            return new ProduceResult(offset, "消息写入失败，状态码：" + status);
        }

        return new ProduceResult(offset, null);
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {   //Contains arrays of messages and error information for fetch operations
        // 1. 读取响应类型
        byte responseType = buffer.get();

        // 2. 处理错误响应
        if (responseType == ERROR_RESPONSE) {
            short errorLength = buffer.getShort();
            byte[] errorBytes = new byte[errorLength];
            buffer.get(errorBytes);

            String error = new String(errorBytes, StandardCharsets.UTF_8);
            return new FetchResult(new byte[0][], error);
        }

        if (responseType != FETCH_RESPONSE) {
            return new FetchResult(new byte[0][], "无效的响应类型：" + responseType);
        }

        // 3. 读取消息数量，创建用于存放消息的二维数组
        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];

        // 4. 按顺序读取每条消息
        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // 读取偏移量，当前结果结构不保存它
            int messageSize = buffer.getInt();

            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages, null);
    }
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) { //Contains broker and topic metadata along with error information
        byte responseType = buffer.get();

        // 1. 处理错误响应
        if (responseType == ERROR_RESPONSE) {
            short errorLength = buffer.getShort();
            byte[] errorBytes = new byte[errorLength];
            buffer.get(errorBytes);

            String error = new String(errorBytes, StandardCharsets.UTF_8);
            return new MetadataResult(
                    new ArrayList<>(), new ArrayList<>(), error);
        }

        if (responseType != METADATA_RESPONSE) {
            return new MetadataResult(
                    new ArrayList<>(),
                    new ArrayList<>(),
                    "无效的响应类型：" + responseType);
        }

        // 2. 读取 Broker 信息
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();

        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();

            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes, StandardCharsets.UTF_8);

            int port = buffer.getInt();

            brokers.add(new BrokerInfo(brokerId, host, port));
        }

        // 3. 读取主题信息
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes, StandardCharsets.UTF_8);

            // 4. 读取当前主题的分区信息
            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();

            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();

                // 5. 读取当前分区各副本所在的 Broker ID
                int replicaCount = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();

                for (int k = 0; k < replicaCount; k++) {
                    replicaIds.add(buffer.getInt());
                }

                partitions.add(new PartitionMetadata(
                        partitionId, leaderId, replicaIds));
            }

            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }

    // 编码副本同步请求：将指定分区、指定 offset 的消息发送给其他 Broker
    public static ByteBuffer encodeReplicateRequest(String topic, int partition, long offset, byte[] message) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length == 0 || topicBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Topic 名称不能为空或过长");
        }
        if (partition < 0 || offset < 0) {
            throw new IllegalArgumentException("分区和偏移量不能为负数");
        }

        ByteBuffer buffer =
                ByteBuffer.allocate(19 + topicBytes.length + message.length);

        buffer.put(REPLICATE);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);

        buffer.flip();
        return buffer;
    }

    // 编码主题通知：告知其他 Broker 某个主题发生了更新
    public static ByteBuffer encodeTopicNotification(String topic) {
        byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);

        if (topicBytes.length == 0 || topicBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Topic 名称不能为空或过长");
        }

        ByteBuffer buffer = ByteBuffer.allocate(3 + topicBytes.length);

        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topicBytes.length);
        buffer.put(topicBytes);

        buffer.flip();
        return buffer;
    }

    public static void sendErrorResponse(
            SocketChannel channel, String errorMessage) throws IOException {
        byte[] errorBytes = errorMessage.getBytes(StandardCharsets.UTF_8);

        if (errorBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("错误信息过长");
        }

        ByteBuffer buffer = ByteBuffer.allocate(3 + errorBytes.length);

        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorBytes.length);
        buffer.put(errorBytes);

        buffer.flip();

        // 一次 write 不一定发送全部字节，需要继续发送剩余内容
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}
