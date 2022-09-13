package com.atguigu.demo.chapter11

import com.atguigu.demo.chapter05.Event
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.Expressions.$
/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-09-08  
 * @Desc
 *      可以看到，所有输出结果都以+I 为前缀，表示都是以 INSERT 操作追加到结果表中的;
 *      这是一个追加查询，所以我们直接使用 toDataStream()转换成流是没有问题的
 */
object AppendQueryExample {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据源，并分配时间戳、生成水位线
    val eventStream = env
      .fromElements(
        Event("Alice", "./home", 1000L),
        Event("Bob", "./cart", 1000L),
        Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
        Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
        Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
        Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
        Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
      )
      .assignAscendingTimestamps(_.timestamp) // 创建表环境
    val tableEnv = StreamTableEnvironment.create(env) // 将数据流转换成表，并指定时间属性
    val eventTable = tableEnv.fromDataStream(
      eventStream,
      $("user"),
      $("url"), $("timestamp").rowtime().as("ts")
      // 将 timestamp 指定为事件时间，并命名为 ts
    )
    // 为方便在 SQL 中引用，在环境中注册表 EventTable
    tableEnv.createTemporaryView("EventTable", eventTable); // 设置 1 小时滚动窗口，执行 SQL 统计查询
    val result = tableEnv
      .sqlQuery(
        "SELECT " +
          "user, " +
          "window_end AS endT, " + // 窗口结束时间
           "COUNT(url) AS cnt " + // 统计 url 访问次数
           "FROM TABLE( " +
          "TUMBLE( TABLE EventTable, " + // 1 小时滚动窗口
           "DESCRIPTOR(ts), " +
          "INTERVAL '1' HOUR)) " +
          "GROUP BY user, window_start, window_end "
      )
    tableEnv.toDataStream(result).print()
    env.execute()
  }

}
