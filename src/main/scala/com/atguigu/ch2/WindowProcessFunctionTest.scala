package com.atguigu.ch2

import java.sql.Timestamp

import com.atguigu.demo.chapter05.{ClickSource, Event}
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object WindowProcessFunctionTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_ =>"key")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new UvCountByWindow)
      .print()

    env.execute()
  }


  class UvCountByWindow extends ProcessWindowFunction[Event,String,String,TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
      var userSet= Set[String]()
      elements.foreach(userSet += _.user)
      val start: Long = context.window.getStart
      val end: Long = context.window.getEnd
      out.collect("窗口:"+ new Timestamp(start)+"~"+new Timestamp(end)+"的独立方可数量是"+userSet.size)
    }
  }


}
