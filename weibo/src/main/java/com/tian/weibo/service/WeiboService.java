package com.tian.weibo.service;

import com.tian.weibo.constant.Names;
import com.tian.weibo.dao.WeiboDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author JARVIS
 * @version 1.0
 * 2019/8/16 10:17
 */
public class WeiboService {

    private WeiboDao dao = new WeiboDao();

    public void init() throws IOException {
        dao.createNameSpace(Names.NAMESPACE_WEIBO);
        dao.createTable(Names.TABLE_WEIBO,Names.WEIBO_FAMILY_DATA);
        dao.createTable(Names.TABLE_RELATION,Names.RELATION_FAMILY_DATA);
        dao.createTable(Names.TABLE_INBOX,Names.INBOX_FAMILY_DATA);
    }

    //发微博
    //再weibo表插入数据，并找出该start的所有fans，再inbox中插入数据
    public void publish(String star, String content) throws IOException {
        //再weibo中插入数据
        String rowKey = star + "_" + System.currentTimeMillis(); //weiboId
        dao.putCell(Names.TABLE_WEIBO, rowKey, Names.WEIBO_FAMILY_DATA, Names.WEIBO_COLUMN_CONTENT, content);
        //查找start的所有fansId
        String prefix = star + ":followedby";
        List<String> list = dao.getRowKeysByPrefix(Names.TABLE_RELATION, prefix);
        if (list.size() <= 0) return;
        List<String> fansIds = new ArrayList<>();
        for (String row : list) {
            String[] split = row.split(":");
            fansIds.add(split[2]);
        }

        //将weiboId插入到所有fans的inbox中
        //多行的同一个列族的同一个列插入相同数据
        dao.putCells(Names.TABLE_INBOX, fansIds, Names.INBOX_FAMILY_DATA, star, rowKey);
    }

    /**
     * 关注
     * 在relation表中插入两条数据
     * 在inbox表中插入star近期的weiboId
     * @param fans
     * @param star
     */
    public void follow(String fans, String star) throws IOException {
        String rowKey = fans + ":fallow:" + star;
        String time = System.currentTimeMillis() +"";
        dao.putCell(Names.TABLE_RELATION,rowKey,Names.RELATION_FAMILY_DATA,Names.RELATION_COLUMN_TIME,time);

        //weibo表结构，rowKey为star加时间戳
        //获取star的近期weiboId
        //通过范围获得star对应的所有weibo的rowKey
        List<String> weiboIds = new ArrayList<>();
        String startRow = star + "_";
        String stopRow = star + "_|";
        weiboIds = dao.getRowKeyesByRange(Names.TABLE_WEIBO,startRow,stopRow);

        //截取三个副本 TODO 优化业务逻辑
        List<String> recentWeiBoIds = weiboIds.subList(weiboIds.size() - Names.INBOX_VERSIONS, weiboIds.size());
        for (String recentWeiBoId : recentWeiBoIds) {
            dao.putCell(Names.TABLE_INBOX,fans,Names.INBOX_FAMILY_DATA,fans,recentWeiBoId);
        }
    }
}
