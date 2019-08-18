package com.tian.weibo.names;

/**
 * 常量类
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/18 18:05
 */
public class Names {
    //命名空间
    public static final String NAMESPACE_WEIBO = "weibo";
    //表名
    public static final String TABLE_RELATION = "relation";
    public static final String TABLE_INBOX = "inbox";
    public static final String TABLE_WEIBO = "weibo";
    //列族
    public static final String FAMILY_WEIBO_INFO = "info";
    public static final String FAMILY_RELATION_INFO = "info";
    public static final String FAMILY_INBOX_INFO = "info";
    //列名
    public static final String COLUMN_WEIBO_CONTENT = "content";
    public static final String COLUMN_RELATION_TIME = "time";
    //版本
    public static final Integer INBOX_VERSION = 3;
}
