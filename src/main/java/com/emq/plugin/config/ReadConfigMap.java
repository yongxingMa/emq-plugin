package com.emq.plugin.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;

public class ReadConfigMap {

    /**
     * 读取一级参数
     * @return
     */
    public static String readJsonParam(String param) {
        //win
//        String path = ReadConfigMap.class.getClassLoader().getResource(FileListener.FileName).getPath();
        //linux
        String path = File.separator+"etc" + File.separator+"config" + File.separator + FileListener.FileName;
        String s = readJsonFile(path);
        JSONObject json = JSON.parseObject(s);
        if (json.get(param) != null) {
            return json.get(param).toString();
        }
        return null;
    }

    /**
     * 读取二级参数
     * @return
     */
    public static String readJsonParam2(String param1,String param2) {
        //win
//        String path = ReadConfigMap.class.getClassLoader().getResource(FileListener.FileName).getPath();
        //linux
        String path =  File.separator+"etc" + File.separator+"config" + File.separator + FileListener.FileName;
        String s = readJsonFile(path);
        JSONObject json = JSON.parseObject(s);
        if (json.getJSONObject(param1).get(param2) != null) {
            return json.getJSONObject(param1).get(param2).toString();
        }
        return null;
    }

    /**
     * 读取json文件，返回json串
     * @param fileName
     * @return
     */
    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile),"utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

}
