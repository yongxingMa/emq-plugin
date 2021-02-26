package com.emq.plugin;

import com.emq.plugin.config.FileMonitor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class App {

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
        FileMonitor fileMonitor = new FileMonitor();
        //开启监听
        fileMonitor.startMonitor();
    }

}
