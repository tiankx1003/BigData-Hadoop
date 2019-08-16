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


}
