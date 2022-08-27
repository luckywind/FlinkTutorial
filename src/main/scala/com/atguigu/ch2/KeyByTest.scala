package com.atguigu.ch2

import com.atguigu.demo.chapter05.Event
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object KeyByTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream=env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )



    val keyd: KeyedStream[Event, String] = stream.keyBy(_.user)
    keyd.print()
    env.execute()

  }
}
