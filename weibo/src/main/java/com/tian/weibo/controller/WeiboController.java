package com.tian.weibo.controller;

import com.tian.weibo.names.Names;
import com.tian.weibo.service.WeiboService;

import java.io.IOException;
import java.util.List;

/**
 * 具体功能层
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/18 18:28
 */
public class WeiboController {
    private WeiboService service = new WeiboService();

    //初始化
    public void init() throws IOException {
        service.init();
    }

    //发微博
    public void publish(String star, String weibo) throws IOException {
        service.publish(star, weibo);
    }

    //关注
    public void follow(String fans, String star) throws IOException {
        service.follow(fans, star);
    }

    //取关
    public void unFollow(String fans, String star) throws IOException {
        service.unFollow(fans, star);
    }

    //获取某个star的所有微博
    public List<String> getWeiboByStar(String star) throws IOException {
        return service.getCellsByPrefix(Names.TABLE_WEIBO,star, Names.FAMILY_WEIBO_INFO,Names.COLUMN_WEIBO_CONTENT);
    }

    //获取某个fans所有star的近期微博
    public List<String> getRecentWeibo(String fans){
        return service.getRecentWeibo(fans);
    }

}
