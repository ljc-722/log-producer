package pub.synx.controller;

import java.util.Map;

import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.shaded.org.slf4j.Logger;
import org.apache.rocketmq.shaded.org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;

@RestController
@RequestMapping
public class ProduceController {

    @Value("${rmq.endpoint}")
    private String endpoint;

    @Value("${rmq.topic}")
    private String topic;

    private static final Logger log = LoggerFactory.getLogger(ProduceController.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // 生产者
    private Producer producer;

    @PostConstruct
    public void initProducer() throws ClientException {
        producer = ClientServiceProvider.loadService().newProducerBuilder()
                .setTopics(topic)
                .setClientConfiguration(
                        ClientConfiguration.newBuilder().setEndpoints(endpoint).build())
                .setMaxAttempts(3)
                .build();
        log.info("Producer initialized");
    }

    @PostMapping("/produce")
    public String produce(@RequestBody Map<String, Object> logData) {
        try {
            log.info("正在对消息进行投递：{}", logData.toString());

            byte[] bytes = objectMapper.writeValueAsBytes(logData);

            // 普通消息发送
            Message message = ClientServiceProvider.loadService().newMessageBuilder()
                    .setTopic(topic)
                    .setBody(bytes)
                    .build();

            SendReceipt sendReceipt = producer.send(message);
            String res = "消息投递成功, messageId={}" + sendReceipt.getMessageId();
            log.info(res);
            producer.close();
            return res;
        } catch (Exception e) {
            log.error("Failed to send message", e);
            return "消息投递失败, 错误原因：" + e.getMessage();
        }
    }
}
