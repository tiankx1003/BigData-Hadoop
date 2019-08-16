package controller;

import service.WeiboService;

import java.io.IOException;
import java.util.List;

public class WeiboController {

    private WeiboService service = new WeiboService();

    public void init() throws IOException {
        service.init();
    }


    //5) 发布微博内容
    public void publish(String star, String content) throws IOException {
        service.publish(star,content);
    }

    //6) 添加关注用户
    public void follow(String fans, String star) {

    }

    //7) 移除（取关）用户
    public void unFollow(String fans, String star) {

    }

    //8) 获取关注的人的微博内容
    //8.1 获取某个star的所有weibo
    public List<String> getWeibosByStarId(String star) {
        return null;
    }

    //8.2 获取某个fans的所有star的近期weibo
    public List<String> getAllRecentWeibos(String fans) {
        return null;
    }

}
