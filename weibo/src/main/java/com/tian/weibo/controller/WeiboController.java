package com.tian.weibo.controller;

import com.tian.weibo.service.WeiboService;

import java.io.IOException;
import java.util.List;

/**
 * 需求
 * 1) 创建命名空间以及表名的定义
 * 2) 创建微博内容表
 * 3) 创建用户关系表
 * 4) 创建用户微博内容接收邮件表
 * 5) 发布微博内容
 * 6) 添加关注用户
 * 7) 移除（取关）用户
 * 8) 获取关注的人的微博内容
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/16 10:14
 */
public class WeiboController {
    private WeiboService service = new WeiboService();

    //初始化
    public void init() throws IOException {
        service.init();
    }

    //发微博
    public void publish(String star, String content) throws IOException {
        service.publish(star,content);
    }

    //关注
    public void follow(String fans, String star) {
        service.follow(fans,star);
    }

    //取关
    public void unFollow(String fans, String star) {

    }

    //获取某个star的所有weibo
    public List<String> getWeibosByStartId() {

        return null;
    }

    //获取某个fans的所有star的近期weibo
    public List<String> getAllRecentWeibos(String fans) {

        return null;
    }


}
