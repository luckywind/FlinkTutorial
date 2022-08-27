package com.atguigu.ch2

import java.util.concurrent.TimeUnit

import com.atguigu.demo.chapter05.Event
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object SinkTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {


     val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)



    val stream = env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=100", 3000L),
      Event("Alice", "./prod?id=200", 3500L),
      Event("Bob", "./prod?id=2", 2500L),
      Event("Alice", "./prod?id=300", 3600L),
      Event("Bob", "./home", 3000L),
      Event("Bob", "./prod?id=1", 2300L),
      Event("Bob", "./prod?id=3", 3300L)
    )


   val fileSink= StreamingFileSink
      .forRowFormat(
        new Path("./output"),
        new SimpleStringEncoder[String]("UTF-8")
      )

     /**
      * 我们就会滚动分区文件:
      * 至少包含15分钟的数据
      *  最近5分钟没有收到新的数据
      *
      *  文件大小已达到 1 GB
      */
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024*(1024*1024))
          .build()
      )
      .build()


    stream.map(_.toString).addSink(fileSink)

    env.execute()



  }
}
