package com.atguigu.ch2


import java.sql.Timestamp

import com.atguigu.demo.chapter05.{ClickSource, Event}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-30  
 * @Desc
 */
object ProcessingTimeTimerTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
     val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new ClickSource)
      .keyBy(r=>true)
      .process(new KeyedProcessFunction[Boolean,Event,String] {
        override def processElement(i: Event,
                                    context: KeyedProcessFunction[Boolean, Event, String]#Context,
                                    collector: Collector[String]): Unit = {
          val currts: Long = context.timerService().currentProcessingTime()
          collector.collect("数据到达："+new Timestamp(currts))
          //注册10s后的处理时间定时器
          context.timerService().registerProcessingTimeTimer(currts+10*1000L)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          out.collect("定时器触发："+new Timestamp(timestamp))
        }
      }

      )
      .print()


    env.execute()
  }
}
