package com.atguigu.ch2

import com.atguigu.demo.chapter05.ClickSource
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object WindowTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env
      .addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .map(r=>(r.user,1L))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5),Time.seconds(1)))
//      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .reduce((r1,r2)=>(r1._1,  (r1._2+r2._2)))
      .print()

    env.execute()
  }
}
