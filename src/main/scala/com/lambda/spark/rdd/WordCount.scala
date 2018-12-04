package com.lambda.spark.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/world_count.text")
    val words = lines.flatMap(line => line.split(" "))

    val wordCount = words.countByValue()
    for((word, count) <- wordCount) println(word + ":" + count)
  }

}
