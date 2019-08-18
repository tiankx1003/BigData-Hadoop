package com.tian.weibo.controller;

import com.tian.weibo.service.WeiboService;

/**
 * 具体功能层
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/18 18:28
 */
public class Controller {
    WeiboService service = new WeiboService();
    //初始化
    public void init(){
        service.init();
    }
}
