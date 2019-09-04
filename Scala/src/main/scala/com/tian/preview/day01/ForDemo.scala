package com.tian.preview.day01

/**
 * @author tian
 *         2019/9/3 20:36
 */
object ForDemo {
    def main(args: Array[String]): Unit = {
        val s1 = "scala"
        for (c <- s1) {
            println(c)
        }
        1 to 10
        1 until 10
        val arr = Array(10, 20, 30, 40)
        for (i <- arr) {
            println(i)
        }
        for (i <- 0 to arr.length - 1) {
            println(arr(i))
        }
        for (i <- 0 until arr.length) {
            println(arr(i))
        }
        for(i <- 1 to 100 by 2){
            println(i)
        }
        for (i <- 1 to (100,2)){
            println(i)
        }
        for(i <- 1 to 100 reverse){
            println(i)
        }
        for(i <- 100 to (1,-1)){
            println(i)
        }
    }

}
