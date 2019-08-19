package com.tian.weibo.controller;

import com.tian.weibo.service.WeiboService;

import java.io.IOException;

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

    //关注
    public void follow(String fans, String star) throws IOException {
        service.follow(fans, star);
    }

    //取关
    public void unFollow(String fans, String star) {

    }
}
