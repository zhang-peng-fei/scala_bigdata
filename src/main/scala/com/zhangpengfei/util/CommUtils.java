package com.zhangpengfei.util;

import java.net.URL;

/**
 *
 * @author 张朋飞
 */
public class CommUtils {


    /**
     * @return 返回类的绝对路径
     */
    public static String getBasicPath() {

        URL basicPath = Thread.currentThread().getContextClassLoader().getResource("");
        return basicPath.toString();
    }


    public static void main(String[] args){
        System.out.println(CommUtils.getBasicPath());
    }
}
