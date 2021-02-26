package com.emq.plugin.plugin;

import com.emq.plugin.config.ReadConfigMap;
import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class emq2ftp implements MqttCallback {

    private emq2ftp(){
    }

    //创建 SingleObject 的一个对象
    private static emq2ftp instance = new emq2ftp();

    //获取唯一可用的对象
    public static emq2ftp getInstance(){
        return instance;
    }

    public String ftpHost;
    public String ftpPort;
    public String ftpUserName;
    public String ftpPassword;
    public String filePath;
    public String fileName;

    FTPClient ftpClient;
    PrintWriter printWriter;
    ExecutorService executorService;
    MqttClient client;

    /**
     * 启动MQTT
     */
    public void start() {
        startFTP();
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
        if(ftpClient != null){
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("ftpClient connect close ");
        }
    }

    public void startFTP() {
        ftpHost = ReadConfigMap.readJsonParam("ftp-ip");
        ftpPort = ReadConfigMap.readJsonParam("ftp-port");
        ftpUserName = ReadConfigMap.readJsonParam("username");
        ftpPassword = ReadConfigMap.readJsonParam("password");
        filePath = ReadConfigMap.readJsonParam("filePath");
        fileName = ReadConfigMap.readJsonParam("fileName");
        ftpClient = new FTPClient();
        // 连接FPT服务器,设置IP及端口
        try {
            // 连接FPT服务器,设置IP及端口
            ftpClient.connect(ftpHost, Integer.parseInt(ftpPort));
            // 登录FTP服务器
            ftpClient.login(ftpUserName, ftpPassword);
            // 设置连接超时时间,5000毫秒
            ftpClient.setConnectTimeout(50000);
            boolean hasDir = ftpClient.changeWorkingDirectory(filePath);
            if (!hasDir) {
                //创建文件夹
                ftpClient.makeDirectory(filePath);
                System.out.println("创建文件夹："+filePath);
                ftpClient.changeWorkingDirectory(filePath);
            }
            // 设置中文编码集，防止中文乱码
            ftpClient.setControlEncoding("UTF-8");
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            System.out.println("FilePath:"+filePath+",fileName:"+fileName);
            //220 连接成功 230 登录成功
            int replyCode = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                System.out.println("连接失败 FTP，用户名或密码错误，replyCode："+replyCode);
                ftpClient.disconnect();
            } else {
                System.out.println("FTP连接成功，replyCode:"+replyCode);
            }
            //向指定文件写入内容，如果没有该文件则先创建文件再写入，写入的方式是追加。
            printWriter = new PrintWriter(new OutputStreamWriter(ftpClient.appendFileStream(fileName), "utf-8"), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 关闭客户端
    public void close()  {
        System.out.println("emq2ftp closing....");
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
        if(ftpClient != null){
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("ftpClient connect close ");
        }
        if(printWriter != null){
            printWriter.close();
            System.out.println("pw connect close ");
        }
    }
    @Override
    public void messageArrived(String topic, MqttMessage message) {
        String data = message.toString();
        executorService.execute(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //数据写入
                printWriter.write(data+"\n");
                System.out.println(Thread.currentThread().getName()+",emq2ftp success: "+data);
                printWriter.flush();
            }
        });
    }
    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}