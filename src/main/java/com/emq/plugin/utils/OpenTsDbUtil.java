package com.emq.plugin.utils;
import org.apache.http.nio.reactor.IOReactorException;
import org.opentsdb.client.OpenTSDBClient;
import org.opentsdb.client.OpenTSDBClientFactory;
import org.opentsdb.client.OpenTSDBConfig;

public class OpenTsDbUtil {

    /**
     * 获取客户端
     */
    public static OpenTSDBClient getClient(String host,Integer port) {
        OpenTSDBClient client = null;
        try {
            OpenTSDBConfig config = OpenTSDBConfig
                    // OpenTsDb数据库地址和端口号
                    .address(host, port)
                    // http连接池大小，默认100
                    .httpConnectionPool(100)
                    // http请求超时时间，默认100s
                    .httpConnectTimeout(100)
                    // 异步写入数据时，每次http提交的数据条数，默认50
                    .batchPutSize(50)
                    // 异步写入数据中，内部有一个队列，默认队列大小20000
                    .batchPutBufferSize(20000)
                    // 异步写入等待时间，如果距离上一次请求超多300ms，且有数据，则直接提交
                    .batchPutTimeLimit(300)
                    // 当确认这个client只用于查询时设置，可不创建内部队列从而提高效率
                    // 每批数据提交完成后回调
                   .config();
            client = OpenTSDBClientFactory.connect(config);
        } catch (IOReactorException e) {
            e.printStackTrace();
        }
        return client;
    }
}
