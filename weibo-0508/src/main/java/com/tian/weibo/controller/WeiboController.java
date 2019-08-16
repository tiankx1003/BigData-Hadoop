package com.tian.weibo.controller;

import com.tian.weibo.constant.Names;
import com.tian.weibo.service.WeiboService;

import java.io.IOException;
import java.util.List;

public class WeiboController {

    private WeiboService service = new WeiboService();

    public void init() throws IOException {
        service.init();
    }


    //5) 发布微博内容
    public void publish(String star, String content) throws IOException {
        service.publish(star, content);
    }

    //6) 添加关注用户
    public void follow(String fans, String star) throws IOException {
        service.follow(fans, star);
    }

    //7) 移除（取关）用户
    public void unFollow(String fans, String star) throws IOException {
        service.unFollow(fans, star);
    }

    //8) 获取关注的人的微博内容
    //8.1 获取某个star的所有weibo
    public List<String> getWeibosByStarId(String star) throws IOException {
        return service.getCellsByPrefix(Names.TABLE_WEIBO, star, Names.WEIBO_FAMILY_DATA, Names.WEIBO_COLUMN_CONTENT);
    }

    //8.2 获取某个fans的所有star的近期weibo
    public List<String> getAllRecentWeibos(String fans) throws IOException {

        return service.getAllRecentWeibos(fans);
    }

}
