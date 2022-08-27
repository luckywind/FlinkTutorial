package com.atguigu.ch2

import com.atguigu.demo.chapter05.{ClickSource, Event}
import org.apache.flink.api.common.functions.AggregateFunction
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object WindowAggTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .map(r=>(r.user,1L))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .aggregate(new AvgPv)
//      .print()


  }





  class AvgPv extends AggregateFunction[Event,(Set[String], Double), Double] {
    override def createAccumulator(): (Set[String], Double) = (Set[String](), 0L)

    override def add(in: Event, acc: (Set[String], Double)): (Set[String], Double) = {
      acc._1.+(in.user)
      acc._2+1L
      acc
    }

    override def getResult(acc: (Set[String], Double)): Double = {
      acc._2/acc._1.size
    }

    override def merge(acc: (Set[String], Double), acc1: (Set[String], Double)): (Set[String], Double) = ???
  }
}
