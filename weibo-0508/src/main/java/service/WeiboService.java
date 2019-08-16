package service;

import constant.Names;
import dao.WeiboDao;

import javax.naming.Name;
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
}
