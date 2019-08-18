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
    WeiboDao dao = new WeiboDao();
    //初始化
    public void init() throws IOException {
        //创建命名空间
        dao.createNamespace(Names.NAMESPACE_WEIBO);
        //创建表
        dao.createTable(Names.TABLE_RELATION,Names.FAMILY_RELATION_INFO);
        dao.createTable(Names.TABLE_INBOX,Names.FAMILY_INBOX_INFO);
        dao.createTable(Names.TABLE_WEIBO,Names.FAMILY_WEIBO_INFO);
    }
}
