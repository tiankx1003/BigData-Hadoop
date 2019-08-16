package com.tian.weibo.service;

import com.tian.weibo.constant.Names;
import com.tian.weibo.dao.WeiboDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WeiboService {

    private WeiboDao dao = new WeiboDao();

    public void init() throws IOException {

        //1) 创建命名空间以及表名的定义
        dao.createNameSpace(Names.NAMESPACE_WEIBO);

        //2) 创建微博内容表
        dao.createTable(Names.TABLE_WEIBO, Names.WEIBO_FAMILY_DATA);

        //3) 创建用户关系表
        dao.createTable(Names.TABLE_RELATION, Names.RELATION_FAMILY_DATA);

        //4) 创建用户微博内容接收邮件表
        dao.createTable(Names.TABLE_INBOX, Names.INBOX_VERSIONS, Names.INBOX_FAMILY_DATA);
    }

    public void publish(String star, String content) throws IOException {

        //1.在weibo表中插入一条数据
        String rowKey = star + "_" + System.currentTimeMillis();
        dao.putCell(Names.TABLE_WEIBO, rowKey, Names.WEIBO_FAMILY_DATA, Names.WEIBO_COLUMN_CONTENT, content);

        //2.查找star的所有fansId
        String prefix = star + ":followedby:";
        List<String> list = dao.getRowKeysByPrefix(Names.TABLE_RELATION, prefix);

        if (list.size() <= 0) return;

        List<String> fansIds = new ArrayList<>();

        for (String row : list) {
            String[] split = row.split(":");
            fansIds.add(split[2]);
        }

        //3.将weiboID插入到所有fans的inbox中
        dao.putCells(Names.TABLE_INBOX, fansIds, Names.INBOX_FAMILY_DATA, star, rowKey);

    }

    public void follow(String fans, String star) throws IOException {

        //1.往relation表中插入两条数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        String time = System.currentTimeMillis() + "";
        dao.putCell(Names.TABLE_RELATION, rowKey1, Names.RELATION_FAMILY_DATA, Names.RELATION_COLUMN_TIME, time);
        dao.putCell(Names.TABLE_RELATION, rowKey2, Names.RELATION_FAMILY_DATA, Names.RELATION_COLUMN_TIME, time);

        //2.从weibo表中获取star的近期weiboID
        String startRow = star + "_";
        //azhas_1561651351651
        String stopRow = star + "_|";
        List<String> list = dao.getRowKeysByRange(Names.TABLE_WEIBO, startRow, stopRow);
        if (list.size() <= 0) {
            return;
        }

        int fromIndex = list.size() >= 3 ? list.size() - Names.INBOX_VERSIONS : 0;
        List<String> recentWeiboIds = list.subList(fromIndex, list.size());

        //3.将star的近期weiboId插入到fans的inbox
        for (String recentWeiboId : recentWeiboIds) {
            dao.putCell(Names.TABLE_INBOX, fans, Names.INBOX_FAMILY_DATA, star, recentWeiboId);
        }
    }

    public void unFollow(String fans, String star) throws IOException {

        //1.删除relation表中的两条数据
        String rowKey1 = fans + ":follow:" + star;
        String rowKey2 = star + ":followedby:" + fans;
        dao.deleteRow(Names.TABLE_RELATION, rowKey1);
        dao.deleteRow(Names.TABLE_RELATION, rowKey2);

        //2.删除inbox中fans行的star列
        dao.deleteColumn(Names.TABLE_INBOX, fans, Names.INBOX_FAMILY_DATA, star);
    }

    public List<String> getCellsByPrefix(String tableName, String prefix, String family, String column) throws IOException {
        return dao.getCellsByPrefix(tableName, prefix, family, column);
    }

    public List<String> getAllRecentWeibos(String fans) throws IOException {


        //1.从inbox表中获取fans的所有star的近期weiboId
        List<String> list = dao.getRow(Names.TABLE_INBOX, fans);

        if (list.size() <= 0) return new ArrayList<>();

        //2.从weibo表中获取相应的weibo内容
        return dao.getCellsByRowKeys(Names.TABLE_WEIBO, list, Names.WEIBO_FAMILY_DATA, Names.WEIBO_COLUMN_CONTENT);
    }
}
