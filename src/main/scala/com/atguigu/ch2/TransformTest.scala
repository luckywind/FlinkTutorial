package com.atguigu.ch2

import com.atguigu.demo.chapter05.Event
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-26  
 * @Desc
 */
object TransformTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L)
    )

    stream.map( new UserExtractor).print("2")

    stream.filter(new UserFilter).print("filter")

    stream.flatMap(new MyFlatMap).print("flat")
    env.execute()

  }


  class UserExtractor extends MapFunction[Event,String]{
    override def map(t: Event): String = t.user
  }


  class UserFilter extends FilterFunction[Event]{
    override def filter(t: Event): Boolean = t.user.equals("Mary")
  }



  class MyFlatMap extends FlatMapFunction[Event,String]{
    override def flatMap(t: Event, collector: Collector[String]): Unit = {
      if (t.user.equals("Mary")) {
        collector.collect(t.user)
      }else if (t.user.equals("Bob")) {
        collector.collect(t.user)
        collector.collect(t.user)
      }
    }
  }
}
