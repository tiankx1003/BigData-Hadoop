package com.tian.preview.day01

import scala.util.control.Breaks
import scala.util.control.Breaks._

/**
 * @author tian
 *         2019/9/3 20:43
 */
object BreakDemo {
    /**
     * 在scala中没有循环的break关键字
     * 通过抛出异常并`try-catch`的方法使循环结束
     * 也可把循环放进方法使用return跳出方法从而跳出循环
     * @param args
     */
    def main(args: Array[String]): Unit = {
        // 原生try-catch
        try {
            for (i <- 1 to 100) {
                println(i)
                if (i == 10) throw new NullPointerException
            }
        } catch {
            case e =>
        }
        println("done")
        // 通过调用方法进行简化
        Breaks.breakable(
            for (i <- 1 to 100) {
                println(i)
                if (i == 10) Breaks.break()
            }
        )
        println("done")
        // 导包后还可简化为
        breakable { // 本质上是try-catch,变圆括号为大括号
            for (i <- 1 to 10){
                println(i)
                if (i == 5) break // 本质是在抛出异常，Break.break的缩写
            }
        }
        println("done")
    }
}
