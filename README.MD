## emq-plugin 数据存储工具
#### 工具介绍
emq-plugin工具，提供了从emq订阅数据（MQTT协议都支持，例如 mosquito），然后将数据转存到第三方存储数据库。
- 目前支持类型有：emq2kafka，emq2mysql，emq2ftp，emq2tsdb。
- 后期会进行不断的更新，也同时欢迎大家参与进来，提供更多的功能支持。
- 如果对您有帮助的话，请点击一下右上方小星星，谢谢。

#### 工作流程
- 1.开启定时器，定时监听指定目录下的json文件。
- 2.监视json文件的创建，更新，删除，会分别触发程序的启动和关闭。
- 3.适用于k8s环境下，该工具创建Deployment,配置文件创建ConfigMap，支持热更新配置。

#### 使用说明
1.windows本地调试
- 1.git clone代码到本地，使用IDEA打开。
- 2.修改 FileMoniter 和 ReadConfigMap中的路径为win下。
- 3.启动App，修改json配置文件，注意：只能有一个json配置文件，而且只能有一个type。
- 4.配置自己服务的地址，启动成功，会看到有发送成功的日志。

2.Linux启动
- 1.将代码maven install，打包成jar文件，注意修改文件读取的路径。
- 2.jar -jar emq-plugin-1.0-SNAPSHOT.jar
- 3.启动成功后，可以看到发送的日志。

3.Docker容器启动
- 1.docker pull smart33690/emq-plugin:1.0
- 2.docker run -d --net=host --name emq-plugin smart33690/emq-plugin:1.0

4.k8s启动
- 1.按照项目中的deployment.yaml配置即可，也可以按需自行配置，仅供参考。

#### 配置文件介绍
#####emq2kafka.json

| 参数 | 说明 |
| ------ | ------ |
|  type| 指定类型，用于启动emq2kafka程序 |
| kafka-ip | kafka的ip，多个ip使用","隔开 | 
| kafka-topic | kafka的topic |
| kafka-key | kafka的key |
| emq-ip | emq的ip，或者其他mqttClient的ip |
| emq-topic | 消息的主题 |
| emq-qos | 消息质量 |

配置文件示例如下：

```
{
  "type": "kafka",
  "kafka-ip": "192.168.111.199:9092",
  "kafka-topic": "test-topic",
  "partition": "1",
  "kafka-key": "kafka-key",
  "emq-ip": "tcp://192.168.111.199",
  "emq-port": "1883",
  "emq-topic": "test-topic",
  "emq-qos": 1
}
```

#####emq2mysql.json

| 参数 | 说明 |
| ------ | ------ |
|  type| 指定类型，用于启动emq2mysql程序 |
| mysql-ip | mysql的地址 | 
| mysql-port | mysql的端口 |
| username | mysql登录用户名 |
| password | mysql登录密码 |
| database | mysql数据库，如没有会自动创建 |
| tableName | mysql的数据表 |
| fields | mysql的数据表的字段，数组 |
| statement | mysql创建语句，用于创建数据表和对应的字段 |
| emq-ip | emq的ip，或者其他mqttClient的ip |
| emq-topic | 消息的主题 |
| emq-qos | 消息质量 |

配置文件示例如下：

```
  "type": "mysql",
  "mysql-ip": "106.38.78.445",
  "mysql-port": "30306",
  "username": "root",
  "password": "root",
  "database": "device445",
  "tableName": "device445",
  "fields": "[tag456, tag22, tag123]",
  "statement": "CREATE TABLE IF NOT EXISTS `device445`(id varchar(50) PRIMARY KEY,`created_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间' , tag456 VARCHAR (50), tag22 VARCHAR (50), tag123 VARCHAR (50))",
  "emq-ip": "tcp://192.168.111.199",
  "emq-port": "1883",
  "emq-topic": "test-topic",
  "emq-qos": 1
}
```

#####emq2ftp.json

| 参数 | 说明 |
| ------ | ------ |
|  type| 指定类型，用于启动emq2ftp程序 |
| ftp-ip | ftp服务器的地址 | 
| ftp-port | ftp服务器的端口 |
| username | ftp登录用户名 |
| password | ftp登录密码 |
| filePath | ftp服务器的的路径，如没有会自动创建 |
| fileName | ftp服务器路径下的文件名，如没有会自动创建|
| emq-ip | emq的ip，或者其他mqttClient的ip |
| emq-topic | 消息的主题 |
| emq-qos | 消息质量 |

配置文件示例如下：

```
{
  "type":"ftp",
  "ftp-ip": "192.168.111.199",
  "ftp-port": "21",
  "username": "test",
  "password": "test",
  "filePath": "/test",
  "fileName": "test9.json",
  "emq-ip": "tcp://192.168.111.199",
  "emq-port": "1883",
  "emq-topic": "test-topic",
  "emq-qos": 1
}
```

#####emq2tsdb.json

| 参数 | 说明 |
| ------ | ------ |
|  type| 指定类型，用于启动emq2tsdb程序 |
| tsdb-host | tsdb的ip | 
| tsdb-port | tsdb的端口 |
| fields | tsdb的tagValue，数组 |
| emq-ip | emq的ip，或者其他mqttClient的ip |
| emq-topic | 消息的主题 |
| emq-qos | 消息质量 |

解释一下这里为什么没有tagKey？
对于使用emq的都是时序数据，一般数据都如下：
{"ElevatorPower1_val":"4998","ElevatorPower2_val":"9996","ElevatorPower3_val":"14994","ElevatorPower4_val":"19992","event_id":"e9d31f2a-b25b-46fa-95fd-414093317a48","timestamp":1605591980060}
都是key：value的格式，这里如果key value代替tagKey，tagValue的话，那数据量太大，不能这么设计。
所以这里我固定了tagKey，tagValue是指数据中的key，当然在使用的时候，可以自行调整。

配置文件示例如下：

```
{
  "type": "tsdb",
  "tsdb-host": "http://192.168.111.199",
  "tsdb-port": "4242",
  "fields": "[tag1]",
  "emq-ip": "tcp://192.168.111.199",
  "emq-port": "1883",
  "emq-topic": "test-topic",
  "emq-qos": 1
}
```
