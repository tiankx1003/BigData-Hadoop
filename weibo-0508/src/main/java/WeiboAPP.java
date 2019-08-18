import controller.WeiboController;

import java.io.IOException;
import java.util.List;

public class WeiboAPP {

    private static WeiboController controller = new WeiboController();

    public static void main(String[] args) throws IOException {

//        controller.init();

//        controller.follow("1001","1002");
//        controller.follow("1001","1003");
//        controller.follow("1001","1004");

//        controller.unFollow("1001","1004");

//        controller.publish("1002", "happy 10.1");
//        controller.publish("1002", "happy 10.2");
//        controller.publish("1002", "happy 10.3");
//        controller.publish("1002", "happy 10.4");
//        controller.publish("1002", "happy 10.5");
//        controller.publish("1002", "happy 10.6");
//        controller.publish("1002", "happy 10.7");

//        List<String> weibos = controller.getWeibosByStarId("1002");
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
