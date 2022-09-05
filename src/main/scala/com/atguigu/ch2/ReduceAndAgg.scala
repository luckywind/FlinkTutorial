package com.atguigu.ch2

import java.util

import com.atguigu.demo.chapter05.{ClickSource, Event}
import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.flink.api.common.functions.AggregateFunction
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-28  
 * @Desc
 */
object ReduceAndAgg {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .aggregate(new UrlViewCountAgg,new UrlViewCountResult)
      .print( )


    env.execute()

  }





  //IN, ACC, OUT
  class UrlViewCountAgg extends AggregateFunction[Event,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: Event, acc: Long): Long = acc+1L

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = ???
  }



//IN, OUT,key, window
  class UrlViewCountResult extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      out.collect(UrlViewCount(
        key,
        elements.iterator.next(),
        context.window.getStart,
        context.window.getEnd
      ))
    }
  }




  case class UrlViewCount(url:String,count:Long, windowStart:Long,windowEnd:Long)
}
