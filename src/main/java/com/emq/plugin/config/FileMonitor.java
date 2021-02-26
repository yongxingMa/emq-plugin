package com.emq.plugin.config;

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;


public class FileMonitor {

    public  void startMonitor()  {
        System.out.println("启动成功，开启监听。。。");
        //win
//        FileAlterationObserver observer = new FileAlterationObserver("C:\\emq-plugin\\src\\main\\resources",FileFilterUtils.suffixFileFilter(".json"));
        //linux
        FileAlterationObserver observer = new FileAlterationObserver("/etc/config/",FileFilterUtils.suffixFileFilter(".json"));
        observer.addListener(new FileListener());
        FileAlterationMonitor monitor = new FileAlterationMonitor(5000, observer);
        try {
            monitor.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
