package com.atguigu.demo.chapter11

import com.atguigu.demo.chapter05.Event
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-09-07  
 * @Desc
 */
object TableExample {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    // 获取流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据源
    val eventStream = env
      .fromElements(
        Event("Alice", "./home", 1000L),
        Event("Bob", "./cart", 1000L),
        Event("Alice", "./prod?id=1", 5 * 1000L),
        Event("Cary", "./home", 60 * 1000L),
        Event("Bob", "./prod?id=3", 90 * 1000L),
        Event("Alice", "./prod?id=7", 105 * 1000L)
      )
    // 获取表环境
    val tableEnv = StreamTableEnvironment.create(env)
    // 将数据流转换成表
    val eventTable = tableEnv.fromDataStream(eventStream)
    // 用执行 SQL 的方式提取数据
    val visitTable = tableEnv.sqlQuery("select url, user from " + eventTable) // 将表转换成数据流，打印输出
//    tableEnv.toDataStream(visitTable).print() // 执行程序

    val cntTable=tableEnv.sqlQuery(
      s"""
        |select user,count(1) cnt from ${eventTable} group by user
        |""".stripMargin)

    tableEnv.toChangelogStream(cntTable)
        .print()




    env.execute()
  }
}
