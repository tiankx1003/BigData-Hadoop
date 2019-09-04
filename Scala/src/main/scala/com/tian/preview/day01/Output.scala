package com.tian.preview.day01

/**
 * @author tian
 *         2019/9/3 20:06
 */
object Output {
    def main(args: Array[String]): Unit = {
        System.out.println("Hello World!")
        printf("Hello World!")
        println("Hello World!")
        var a = 20
        var b = 201.1
        printf("Print %d %f %.2f %s",a,b,b,"Hello World!")
        // 字符模板输出，可以实现换行，粘贴保持原有格式
        val s =
            """
              |insert into table ads_continuity_uv_count
              |select
              |	'2019-08-28' dt,
              |	concat_ws(date_add('2019-08-28',-6),'_','2019-08-28') wk_dt,
              |	count(*) continuity_count
              |from
              |	(
              |		select mid_id
              |		from
              |			(
              |				select mid_id
              |				from
              |					(
              |						-- 求活跃日期和序号的差值，差值一致则为连续活跃
              |						select mid_id,date_sub(dt,rank) date_dif
              |						from
              |							(
              |								-- 开窗，为每个mid_id按照活跃的日期分配序号
              |								select mid_id,dt,rank() over(partition by mid_id order by dt) rank
              |								from dws_uv_day
              |								where dt between date_add('2019-08-28',-6) and '2019-08-28'
              |							) t1
              |					) t2
              |				group by mid_id, date_dif
              |				having count(*)>=3
              |			) t3
              |		group by mid_id -- 去重，有123,567两次连续三天的用户，这种只计一次
              |	) t4;
              |""".stripMargin
        val sa = 10
        val ss = "student_table"
        val s2 = //可以调用变量
            s"""
              |select name,age
              |from ${ss}
              |where age=${sa};
              |""".stripMargin
        val s3 = s"age = ${sa}"
        println(s)
        println(s2)
        println(s3)
    }

}
