package com.tian.preview.day01

/**
 * @author tian
 *         2019/9/3 20:31
 */
object IfDemo {
    def main(args: Array[String]): Unit = {
        val m = 10
        val n = 20
        if (m > n) {
            println(m)
        } else if (m < n) {
            println(n)
        } else {
            println("equal")
        }

        val j = if (3 < 2) 10 else if (3 < 1) 20 else 30
        val i = if (m > n) m else n;
    }
}
