package com.emq.plugin.plugin;

import com.emq.plugin.config.ReadConfigMap;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class emq2kafka implements MqttCallback {

    private emq2kafka(){
    }

    /**
     * kafka-props
     */
    static Properties props = new Properties();

    Producer<String, String> kafkaProducer;

    //创建 SingleObject 的一个对象
    private static emq2kafka instance = new emq2kafka();

    //获取唯一可用的对象
    public static emq2kafka getInstance(){
        return instance;
    }

    public String kafkaTopic;
    public String kafkaIp;
    public String kafkaKey;
    public String kafkaPartition;
    ExecutorService executorService;

    MqttClient client;

    /**
     * 启动MQTT
     */
    public void start() {
        startKafka();
        executorService = Executors.newFixedThreadPool(10);
        String clientId = UUID.randomUUID().toString().replace("-","");
        try {
            client = new MqttClient(ReadConfigMap.readJsonParam("emq-ip"), clientId, new MemoryPersistence());
            // MQTT 连接选项
            MqttConnectOptions connOpts = new MqttConnectOptions();
            // 保留会话
            connOpts.setCleanSession(true);
            // 设置超时时间
            connOpts.setConnectionTimeout(60);
            // 设置会话心跳时间
            connOpts.setKeepAliveInterval(60);
            // 设置自动重连
            connOpts.setAutomaticReconnect(true);
            // 设置回调
            client.setCallback(this);
            // 建立连接
            System.out.println("Mqtt Connecting to broker success!");
            client.connect(connOpts);
            // 订阅
            client.subscribe(ReadConfigMap.readJsonParam("emq-topic"),1);
        }
        catch (MqttException me) {
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("connect losting... ");
        if(client != null) {
            try {
                client.disconnect();
                client.close();
                System.out.println("mqtt connect close ");
            }
            catch (MqttException e) {
                e.printStackTrace();
            }
        }
        if(executorService != null){
            executorService.shutdown();
            System.out.println("executorService shutdown ");
        }
        if(kafkaProducer != null){
            kafkaProducer.close();
            System.out.println("kafka producer connect close ");
        }
    }

    public void startKafka() {
        kafkaTopic = ReadConfigMap.readJsonParam("kafka-topic");
        kafkaIp = ReadConfigMap.readJsonParam("kafka-ip");
        kafkaPartition = ReadConfigMap.readJsonParam("partition");
        kafkaKey = ReadConfigMap.readJsonParam("kafka-key");

        props.put("bootstrap.servers",kafkaIp);
        // acks=0：如果设置为0，生产者不会等待kafka的响应。
        // acks=1：这个配置意味着kafka会把这条消息写到本地日志文件中，但是不会等待集群中其他机器的成功响应。
        // acks=all：这个配置意味着leader会等待所有的follower同步完成。这个确保消息不会丢失，除非kafka集群中所有机器挂掉。这是最强的可用性保证。
        props.put("acks", "1");
        //配置为大于0的值的话，客户端会在消息发送失败时重新发送
        props.put("retries", 0);
        //当多条消息需要发送到同一个分区时，生产者会尝试合并网络请求。这会提高client和生产者的效率
        props.put("batch.size", 163840);
        props.put("linger.ms", 100);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer= new KafkaProducer<>(props);
        System.out.println("Kafka Connecting success!");
    }

    // 关闭客户端
    public void close()  {
        System.out.println("emq2kafka closing....");
        if(client != null) {
            try {
                client.disconnect();
                client.close();
                System.out.println("mqtt connect close ");
            }
            catch (MqttException e) {
                e.printStackTrace();
            }
        }
        if(executorService != null){
            executorService.shutdown();
            System.out.println("executorService shutdown ");
        }
        if(kafkaProducer != null){
            kafkaProducer.close();
            System.out.println("kafka producer connect close ");
        }
    }
    @Override
    public void messageArrived(String topic, MqttMessage message) {
//        System.out.println("Receive Message: "+message.toString());
        String data = message.toString();
        executorService.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName());
                kafkaProducer.send(new ProducerRecord<>(kafkaTopic, data), (metadata, exception) -> {
                            if (exception != null) {
                                System.out.println("--emq to kafka failed :" + exception.getMessage());
                            } else {
                                System.out.println("--emq to Kafka success:" + data);
                            }
                        }
                );
            }
        });
    }
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}