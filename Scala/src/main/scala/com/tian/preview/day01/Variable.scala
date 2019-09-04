package com.tian.preview.day01

/**
 * @author tian
 *         2019/9/3 19:58
 */
object Variable {
    /**
     * 在实际开发中，虽然定义变量的场景很多，
     * 但我们很少为变量重新赋值，而是当作常量来用，
     * 所以在使用scala编程时有限使用val，即能用常量的地方不用变量。
     *
     * @param args
     */
    def main(args: Array[String]): Unit = {
        var a: Int = 10
        val b: Int = 20
        val c = false
        val s = "Hello World!"
        println(a)
        println(b)
        println(c)
        println(s)
        val ` ` = 10
        println(` `)
        val `def` = 10
        val `double` = 11.1
        val `$%%^^###` = "anything"

        val byte:Byte = 10
        val int:Int = byte
        val any:Any = int
        val unit = for(i <- 1 to 2) 10
//        val e = throw NullPointerException
        val byte2 = int.toByte
        val ss:Boolean = int.toString.toBoolean
        val toto = ss.toString.toInt.toDouble.toByte
    }

}
