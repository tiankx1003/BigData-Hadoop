package com.tian.weibo.constant;

/**
 * 新建表时用到的所有常量类
 *
 * @author JARVIS
 * @version 1.0
 * 2019/8/16 10:24
 */
public class Names {
    public static final String NAMESPACE_WEIBO = "weibo";

    public static final String TABLE_WEIBO = "weibo:weibo";  //带有命名空间的表名
    public static final String TABLE_RELATION = "weibo:relation";
    public static final String TABLE_INBOX = "weibo:inbox";

    public static final String WEIBO_FAMILY_DATA = "data";
    public static final String RELATION_FAMILY_DATA = "data";
    public static final String INBOX_FAMILY_DATA = "data";

    public static final String WEIBO_COLUMN_CONTENT = "content";
    public static final String RELATION_COLUMN_TIME = "time";

    public static final String INBOX_VERSIONS = "3";

}
