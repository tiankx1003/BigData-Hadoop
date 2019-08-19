package com.tian.weibo.service;

import com.tian.weibo.dao.WeiboDao;
import com.tian.weibo.names.Names;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 实现主要逻辑
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/18 18:23
 */
public class WeiboService {
    private WeiboDao dao = new WeiboDao();

    //初始化
    public void init() throws IOException {
        //创建命名空间
        dao.createNamespace(Names.NAMESPACE_WEIBO);
        //创建表
        dao.createTable(Names.TABLE_RELATION, Names.FAMILY_RELATION_INFO);
        dao.createTable(Names.TABLE_INBOX, Names.FAMILY_INBOX_INFO);
        dao.createTable(Names.TABLE_WEIBO, Names.FAMILY_WEIBO_INFO);
    }

    //关注
    public void follow(String fans, String star) throws IOException {
        //relation表中插入两条数据(关注与被关注)
        String tableName = Names.TABLE_RELATION;
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        String family = "info";
        String column = Names.COLUMN_RELATION_TIME;
        String time = System.currentTimeMillis() + "";
        dao.putCells(tableName, rowKey1, family, column, time);
        dao.putCells(tableName, rowKey2, family, column, time);
        //inbox表中插入数据，fans所关注的star所发的weibo
        //获取star近期发布的weiboid，按范围获取
        String startRow = star + "_";
        String stopRow = star + "_|"; //设置范围要足够精确
        List<String> rowKeyList = dao.getRowKeysByRange(Names.TABLE_WEIBO, startRow, stopRow);
        if (rowKeyList.size() <= 0) return;
        //截取最新三天数据
        int fromIndex = rowKeyList.size() >= 3 ? rowKeyList.size() - Names.INBOX_VERSION : 0;
        List<String> recentWeibos = rowKeyList.subList(fromIndex, rowKeyList.size());
        for (String weiboid : recentWeibos) {
            dao.putCells(Names.TABLE_INBOX, fans, Names.FAMILY_INBOX_INFO, star, weiboid);
        }
    }

    //取关
    public void unFollow(String fans, String star) throws IOException {
        //删除relation表中的两行数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        dao.deleteRow(Names.TABLE_RELATION, rowKey1);
        dao.deleteRow(Names.TABLE_RELATION, rowKey2);

        //删除inbox表中star对应列的数据
        dao.deleteColumn(Names.TABLE_WEIBO, fans, Names.FAMILY_WEIBO_INFO, star);
    }

    //发微博
    public void publish(String star, String weibo) throws IOException {
        //weibo表插入数据
        String rowKey = star + "_" + System.currentTimeMillis();
        dao.putCells(Names.TABLE_WEIBO, rowKey, Names.FAMILY_WEIBO_INFO, Names.COLUMN_WEIBO_CONTENT, weibo);
        //inbox表中多个fans对应的列插入数据
        //获取star对应的所有fans
        String prefix = star + ":followedby:";
        List<String> fansList = new ArrayList<>();
        fansList = dao.getRowKeysByPrefix(Names.TABLE_RELATION, prefix);
        dao.putCells(Names.TABLE_INBOX, fansList, Names.FAMILY_INBOX_INFO, star, weibo);
    }

    //通过前缀获取数据值
    public List<String> getCellsByPrefix(String tableName, String prefix, String family, String column) throws IOException {
        return dao.getCellsByPrefix(tableName,prefix,family,column);
    }

    public List<String> getRecentWeibo(String fans) throws IOException {
        List<String> allWeibo = new ArrayList<>();
        //获取fans对应的所有格star
        String prefix = fans + ":follow:";
        List<String> starList = new ArrayList<>();
        starList = dao.getRowKeysByPrefix(Names.TABLE_RELATION,prefix);
        //获取每个star对应的近期weibo
        for (String star : starList) {
            List<String> weiboList = getCellsByPrefix(Names.TABLE_WEIBO, star,
                    Names.FAMILY_WEIBO_INFO, Names.COLUMN_WEIBO_CONTENT);
            int startIndex = weiboList.size()>=3?weiboList.size()-Names.INBOX_VERSION:0;
            List<String> recentWeibo = weiboList.subList(startIndex, weiboList.size());
            allWeibo.addAll(recentWeibo);
        }
        return allWeibo;
    }
}
