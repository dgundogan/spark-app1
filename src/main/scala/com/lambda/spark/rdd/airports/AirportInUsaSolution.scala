package com.lambda.spark.rdd.airports

import com.lambda.spark.rdd.common.Utils
import org.apache.spark.{SparkConf, SparkContext}


//Problem
//Create a spark program
//    to read the airport data from in/airport.text
//    find all the airports which are located in United States
//    output the airport's name and the city's name to out/airport_in_usa.text


object AirportInUsaSolution {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/airports.text")
    val airportsInUsa = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")


    val airportsNameAndCityNames = airportsInUsa.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(2)
    })

    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa3.text")
  }
}
