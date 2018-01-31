package com.citi.data

import org.apache.spark.{SparkConf, SparkContext}

object ReadCsvData {

  def main(args: Array[String]){

    val conf = new SparkConf().setAppName("Citigroup SparkCSV Reader").setMaster("spark://168.72.245.87:7077")
    val spark = new SparkContext(conf)
    val names = spark.textFile("C:\\Users\\hs31777\\Desktop\\Spark\\names.csv")

    val st = System.currentTimeMillis()

    println("**************************************************** Total Count of names:"+names+" :"+names.count)

    val namesSplit= names.map(line => line.split(","))
    println("**************************************************** Total Count of namesSplit:"+namesSplit.count)

    val davids =  namesSplit.filter(
      row => row(1).contains("DAVID")
    )
    println("**************************************************** Total Count of DAVID's:"+davids.count)

    val newDavidRows = davids.map (
      row => row(2)
    )

    println("**************************************************** Total Count of newDavidRows:"+newDavidRows.count)

    newDavidRows.collect().foreach(println _)
    Thread.sleep(10000)

    val et = System.currentTimeMillis()
    println("""**************************************************** Total Time="""+(et-st)+" ms")

    spark.stop()
  }
}