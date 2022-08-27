package com.atguigu.ch2

import com.atguigu.demo.chapter05.Event
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object RichFuncTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L)
    )
      .map(new RichMapFunction[Event,Long] {

        override def open(parameters: Configuration): Unit = {
          println("索引为:"+getRuntimeContext.getIndexOfThisSubtask+"的任务开始")
        }

        override def close(): Unit = {
          println("索引为:"+getRuntimeContext.getIndexOfThisSubtask+"的任务结束")

        }

        override def map(in: Event): Long = {
          in.timestamp
        }
      })

    env.execute()

  }
}
