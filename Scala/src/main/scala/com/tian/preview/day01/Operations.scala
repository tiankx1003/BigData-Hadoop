package com.tian.preview.day01

/**
 * @author tian
 *         2019/9/3 20:25
 */
object Operations {
    def main(args: Array[String]): Unit = {
        val s1 = "a"
        val s2 = "a"
        val s0 = ""
        println(s1 + s0 == s2 + s0)
        println((s1 + s0).eq(s2 + s0))
        val a = 10
        val b = 2
        val c = a + 3
        val d = b.+(3)
        val max=if(a>b) a else b
        println(max)
    }
}
