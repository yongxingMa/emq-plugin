package com.emq.plugin.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


@Slf4j
public class MysqlConnection {

    /**
     * url
     */
    private final String url;

    /**
     * user
     */
    private final String user;

    /**
     * password
     */
    private final String password;

    /**
     * Connection
     */
    private Connection conn = null;

    /**
     * PreparedStatement
     */
    private PreparedStatement pst = null;


    /**
     * mysql连接
     *
     * @param url       url
     * @param user      user
     * @param password  password
     * @param createSql createSql
     * @param dateBase  dateBase
     */
    public MysqlConnection(String url, String user, String password, String dateBase, String createSql) {
        this.url = url;
        this.user = user;
        this.password = password;
        try {
            //指定连接类型
            Class.forName("com.mysql.cj.jdbc.Driver");
            //获取连接
            conn = DriverManager.getConnection(url, user, password);
            //数据库不存在创建数据库
            String checkDb = "create database if not exists " + dateBase;
            pst = conn.prepareStatement(checkDb);
            pst.executeUpdate();
            pst.close();
            System.out.println("create database success .");
            pst = conn.prepareStatement("use " + dateBase);
            pst.executeUpdate();
            pst.close();
            //准备执行语句
//            DatabaseMetaData metaData = conn.getMetaData();
            //表不存在
            pst = conn.prepareStatement(createSql);
            System.out.println("create table success .");
            pst.execute();
            pst.close();
        } catch (Exception e) {
            System.out.println(("Mysql Connection error."));
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (this.conn != null) {
                this.conn.close();
            }
        } catch (SQLException e) {
            log.error("disconnect error.");
        }
    }

    /**
     * 获取连接
     * @return Connection
     */
    public Connection getConnection() {
        return conn;
    }
}
