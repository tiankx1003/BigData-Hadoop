package com.tian.weibo.service;

import com.tian.weibo.dao.WeiboDao;
import com.tian.weibo.names.Names;

import java.io.IOException;

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
        dao.putCell(tableName, rowKey1, family, column, time);
        dao.putCell(tableName, rowKey2, family, column, time);
        //inbox表中插入数据，fans所关注的star所发的weibo
        //获取star发布的weiboid

    }
}
