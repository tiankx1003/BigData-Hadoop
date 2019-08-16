import com.tian.weibo.controller.WeiboController;

import java.io.IOException;
import java.util.List;

public class WeiboAPP {

    private static WeiboController controller = new WeiboController();

    public static void main(String[] args) throws IOException {

//        com.tian.weibo.controller.init();

//        com.tian.weibo.controller.follow("1001","1002");
//        com.tian.weibo.controller.follow("1001","1003");
//        com.tian.weibo.controller.follow("1001","1004");

//        com.tian.weibo.controller.unFollow("1001","1004");

//        com.tian.weibo.controller.publish("1002", "happy 10.1");
//        com.tian.weibo.controller.publish("1002", "happy 10.2");
//        com.tian.weibo.controller.publish("1002", "happy 10.3");
//        com.tian.weibo.controller.publish("1002", "happy 10.4");
//        com.tian.weibo.controller.publish("1002", "happy 10.5");
//        com.tian.weibo.controller.publish("1002", "happy 10.6");
//        com.tian.weibo.controller.publish("1002", "happy 10.7");

//        List<String> weibos = com.tian.weibo.controller.getWeibosByStarId("1002");
//        if (weibos.size() >= 1) {
//            for (String weibo : weibos) {
//                System.out.println("weibo = " + weibo);
//            }
//        }

        controller.publish("1003","unHappy 10.1");
        controller.publish("1003","unHappy 10.2");
        controller.publish("1003","unHappy 10.3");
        controller.publish("1003","unHappy 10.4");
        controller.publish("1003","unHappy 10.5");

        List<String> weibos = controller.getAllRecentWeibos("1001");

        if (weibos.size() >= 1){
            for (String weibo : weibos) {
                System.out.println("weibo = " + weibo);
            }
        }
    }
}
