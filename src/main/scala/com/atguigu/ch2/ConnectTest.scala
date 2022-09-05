package com.atguigu.ch2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-30  
 * @Desc
 */
object ConnectTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1: DataStream[Int] = env.fromElements(1,2,3)
    val stream2: DataStream[Long] = env.fromElements(1L,2L,3L,4L)
    val connectedStream: ConnectedStreams[Int, Long] = stream1.connect(stream2)
    val result=connectedStream
      .map(new CoMapFunction[Int,Long,String] {
        override def map1(in1: Int): String = {
          "Int:"+in1
        }

        override def map2(in2: Long): String = {
          "Long:"+in2
        }
      })

    result.print()
    env.execute()
  }
}
