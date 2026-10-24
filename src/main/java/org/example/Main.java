package org.example;

public class Main {
    public static void main(String[] args) {
        byte[] message = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        java.nio.ByteBuffer buffer =
                com.simplekafka.broker.Protocol.encodeProduceRequest(
                        "test", 0, message);

        System.out.println("请求总字节数：" + buffer.remaining());
        System.out.println("请求类型：" + buffer.get());
        System.out.println("Topic 字节长度：" + buffer.getShort());
    }
}