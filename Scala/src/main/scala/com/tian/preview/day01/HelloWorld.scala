package com.tian.preview.day01

/**
 * @author tian
 *       2019/9/3 19:28
 */
object HelloWorld {
    def main(args: Array[String]): Unit = {
        System.out.println("Hello World!")
        println("Hello World!")
        printf("Hello World!")
        val a = 10
        val b = 10.1
        val s = "Hello World!"
        printf("Print: %d %f %.2f %s",a,b,b,s)
    }
}
