package com.emq.plugin.plugin;

import com.alibaba.fastjson.JSONObject;
import com.emq.plugin.config.ReadConfigMap;
import com.emq.plugin.utils.MysqlConnection;
import lombok.SneakyThrows;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class emq2mysql implements MqttCallback {

    private emq2mysql(){}

    //创建 SingleObject 的一个对象
    private static emq2mysql instance = new emq2mysql();

    //获取唯一可用的对象
    public static emq2mysql getInstance(){
        return instance;
    }

    public String tbName;
    public String strFileds;
    public String[] fields;
    ArrayList<String> fieldList = new ArrayList(10);

    PreparedStatement preparedStatement;

    MysqlConnection mysqlConnection;
    MqttClient client;
    ExecutorService executorService ;

    /**
     * 启动MQTT
     */
    public void start() {
        startMysql();
        tbName = ReadConfigMap.readJsonParam("tableName");
        strFileds = ReadConfigMap.readJsonParam("fields");
        fields = strFileds.substring(1,strFileds.length()-1).split(",");
        executorService = Executors.newFixedThreadPool(10);
        for(String s:fields){
            fieldList.add(s.trim());
        }

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
        } catch (MqttException me) {
        }
    }


    @Override
    public void connectionLost(Throwable throwable) {
        System.out.println("mqtt connect lost ");
        mysqlConnection.close();
        System.out.println("mysql connect lost ");
//        executorService.shutdown();

    }

    public void startMysql() {
        String url = "jdbc:mysql://" + ReadConfigMap.readJsonParam("mysql-ip") + ":"
                + ReadConfigMap.readJsonParam("mysql-port") + "?serverTimezone=GMT%2B8&characterEncoding=utf-8&useSSL=false&autoReconnect=true&failOverReadOnly=false";
        mysqlConnection = new MysqlConnection(url, ReadConfigMap.readJsonParam("username"), ReadConfigMap.readJsonParam("password"), ReadConfigMap.readJsonParam("database"),
                ReadConfigMap.readJsonParam("statement"));
    }

    // 关闭客户端
    public void close() {
        System.out.println("emq2mysql closing....");
        try {
            if(client != null) {
                client.disconnect();
                System.out.println("mqtt connect close ");
            }
            if(mysqlConnection != null){
                mysqlConnection.close();
                System.out.println("mysql connect close ");
            }
            if(preparedStatement !=null){
                preparedStatement.close();
                System.out.println("preparedStatement connect close ");
            }
            if(executorService != null){
                executorService.shutdown();
                System.out.println("executorService shutdown ");
            }
        } catch (MqttException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws SQLException {
//            System.out.println("Receive mqtt topic: " + topic+" Qos: "+message.getQos()+",message:"+message.toString());
            executorService.execute(new Runnable() {
                @SneakyThrows
                @Override
                public void run() {
                    String strMessage = message.toString();
                    JSONObject jsonObject = JSONObject.parseObject(strMessage);
                    String eventId = jsonObject.getString("event_id");
                    Timestamp timestamp = new Timestamp(jsonObject.getLong("timestamp"));
                    StringBuilder item = new StringBuilder();
                    StringBuilder itemValue = new StringBuilder();
                    for (String field : fieldList){
                        item.append(field);
                        item.append(",");
                        itemValue.append("\""+jsonObject.get(field)+"\"");
                        itemValue.append(",");
                    }
                    String sql = "insert into "+tbName+"(id,"+item.toString()+"created_time) values (\""+eventId+"\","+itemValue+"\""+timestamp+"\")";
                    System.out.println(Thread.currentThread().getName()+"---"+sql);
                    fieldList.clear();
                    preparedStatement = mysqlConnection.getConnection().prepareStatement(sql);
                    preparedStatement.execute();
//                    preparedStatement.close();
                }
            });
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}
