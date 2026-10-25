package org.example;

import com.simplekafka.broker.Protocol;

import java.nio.ByteBuffer;

public class Main {
    public static void main(String[] args) {
        // 模拟 Broker 返回：消息写入成功，offset 为 10
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(10L);
        response.put((byte) 0);
        response.flip();

        Protocol.ProduceResult result =
                Protocol.decodeProduceResponse(response);

        System.out.println("是否成功：" + result.isSuccess()); // true
        System.out.println("消息偏移量：" + result.getOffset()); // 10
        System.out.println("错误信息：" + result.getError());    // null
    }
}