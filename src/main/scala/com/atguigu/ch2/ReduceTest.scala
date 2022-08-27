package com.atguigu.ch2

import com.atguigu.demo.chapter05.ClickSource
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

/**
 * Copyright (c) 2015 XiaoMi Inc. All Rights Reserved. 
 *
 * @author chengxingfu <chengxingfu@xiaomi.com>
 * @Date 2022-08-27  
 * @Desc
 */
object ReduceTest {
  val defaultPartion = 200
  val defaultShufflePartion = 500
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .map(r=>(r.user,1L))
      .keyBy(_._1)
      .reduce((r1,r2)=>{
        (r1._1,  r1._2+r2._2)
      })
      .keyBy(_ =>true)  //所有数据分到同一个分区
      .reduce((r1,r2) => if (r1._2>r2._2) r1 else r2)
      .print()

    env.execute()


  }
}
