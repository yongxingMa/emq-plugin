package com.emq.plugin.plugin;

import com.alibaba.fastjson.JSONObject;
import com.emq.plugin.config.ReadConfigMap;
import com.emq.plugin.utils.OpenTsDbUtil;
import lombok.SneakyThrows;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.opentsdb.client.OpenTSDBClient;
import org.opentsdb.client.bean.request.Point;

import java.io.*;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class emq2tsdb implements MqttCallback {

    private emq2tsdb(){
    }

    //创建 SingleObject 的一个对象
    private static emq2tsdb instance = new emq2tsdb();

    //获取唯一可用的对象
    public static emq2tsdb getInstance(){
        return instance;
    }

    public String tsdbHost;
    public String tsdbPort;
    public String strFileds;
    public String[] fields;

    ArrayList<String> fieldList = new ArrayList(20);

    OpenTSDBClient openTSDBClient;

    ExecutorService executorService;
    MqttClient client;

    /**
     * 启动MQTT
     */
    public void start() {
        startTSDB();
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
            client.connect(connOpts);
            // 订阅
            client.subscribe(ReadConfigMap.readJsonParam("emq-topic"),1);
            System.out.println("Mqtt Connecting to broker success!");
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
        if(openTSDBClient != null){
            try {
                openTSDBClient.gracefulClose();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("ftpClient connect close ");
        }
    }

    public void startTSDB() {
        tsdbHost = ReadConfigMap.readJsonParam("tsdb-host");
        tsdbPort = ReadConfigMap.readJsonParam("tsdb-port");
        strFileds = ReadConfigMap.readJsonParam("fields");
        fields = strFileds.substring(1,strFileds.length()-1).split(",");
        for(String s:fields){
            fieldList.add(s.trim());
        }
        openTSDBClient = OpenTsDbUtil.getClient(tsdbHost,Integer.parseInt(tsdbPort));
        System.out.println("OpenTSDB Connecting success!");
    }

    // 关闭客户端
    public void close()  {
        System.out.println("emq2tsdb closing....");
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
        if(openTSDBClient != null){
            try {
                openTSDBClient.gracefulClose();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("openTSDBClient connect close ");
        }
    }
    @Override
    public void messageArrived(String topic, MqttMessage message) {
        String strMessage = message.toString();

        executorService.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                JSONObject jsonObject = JSONObject.parseObject(strMessage);
                String device = jsonObject.getString("device");
                for (String field : fieldList){
                    //创建数据对象
                    Point point = Point.metric(device).tag("property",field).value(jsonObject.getLong("timestamp"),jsonObject.getIntValue(field)).build();
                    //将对象插入数据库
                    openTSDBClient.put(point);
                    System.out.println(Thread.currentThread().getName()+" : "+point);
                }
            }
        });
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}