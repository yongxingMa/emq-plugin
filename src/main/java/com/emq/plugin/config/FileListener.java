package com.emq.plugin.config;

import com.emq.plugin.plugin.emq2ftp;
import com.emq.plugin.plugin.emq2mysql;
import com.emq.plugin.plugin.emq2tsdb;
import com.emq.plugin.plugin.emq2kafka;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import java.io.File;

public class FileListener extends FileAlterationListenerAdaptor {
    public static String FileName;

    public FileListener()  {
    }

    @Override
    public void onStart(FileAlterationObserver observer) {
//        System.out.println("时间:"+System.currentTimeMillis()+"==开启监听 start...");
    }

    @Override
    public void onFileCreate(File file) {
        System.out.println("时间:"+System.currentTimeMillis()+file.getName() + "==文件创建,开启同步...");
        this.FileName=file.getName();
        switch(ReadConfigMap.readJsonParam("type")){
            case "mysql" :
                emq2mysql.getInstance().start();
                break;
            case "ftp" :
                emq2ftp.getInstance().start();
                break;
            case "tsdb" :
                emq2tsdb.getInstance().start();
                break;
            case "kafka" :
                emq2kafka.getInstance().start();
                break;
        }
    }

    @Override
    public void onFileChange(File file) {
        System.out.println("时间:"+System.currentTimeMillis()+file.getName() + "==文件更新，开启同步...");
        this.FileName=file.getName();
        switch(ReadConfigMap.readJsonParam("type")){
            case "mysql" :
                emq2mysql.getInstance().close();
                emq2mysql.getInstance().start();
                break;
            case "ftp" :
                emq2ftp.getInstance().close();
                emq2ftp.getInstance().start();
                break;
            case "tsdb" :
                emq2tsdb.getInstance().close();
                emq2tsdb.getInstance().start();
                break;
            case "kafka" :
                emq2kafka.getInstance().close();
                emq2kafka.getInstance().start();
                break;
        }
    }

    @Override
    public void onFileDelete(File file) {
        System.out.println("时间:"+System.currentTimeMillis()+file.getName() + "==文件已被删除...");
            emq2mysql.getInstance().close();
            emq2ftp.getInstance().close();
            emq2tsdb.getInstance().close();
            emq2kafka.getInstance().close();
    }

    @Override
    public void onStop(FileAlterationObserver observer) {
//        System.out.println("时间:"+System.currentTimeMillis()+"==关闭监听 stop...");
    }
}
