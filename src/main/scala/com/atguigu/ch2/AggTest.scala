package com.atguigu.ch2

import com.atguigu.demo.chapter05.Event
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object AggTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream=env.fromElements(
      ("a", 1), ("a", 3), ("b", 3), ("b", 4)
    )




    stream.keyBy(_._1).sum(1).print() //对元组的索引 1 位置数据求和
    stream.keyBy(_._1).sum("_2").print() //对元组的第 2 个位置数据求和
    stream.keyBy(_._1).max(1).print() //对元组的索引 1 位置求最大值
    stream.keyBy(_._1).max("_2").print() //对元组的第 2 个位置数据求最大值
    stream.keyBy(_._1).min(1).print() //对元组的索引 1 位置求最小值
    stream.keyBy(_._1).min("_2").print() //对元组的第 2 个位置数据求最小值
    stream.keyBy(_._1).maxBy(1).print() //对元组的索引 1 位置求最大值
    stream.keyBy(_._1).maxBy("_2").print() //对元组的第 2 个位置数据求最大值
    stream.keyBy(_._1).minBy(1).print() //对元组的索引 1 位置求最小值
    stream.keyBy(_._1).minBy("_2").print() //对元组的第 2 个位置数据求最小值


    val stream1 = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )
    // 使用 user 作为分组的字段，并计算最大的时间戳
    stream1.keyBy(_.user).max("timestamp").print()


    env.execute()
  }
}
