package com.rs.Preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object trimData {
	def main(args: Array[String]): Unit = {
			//			System.setProperty("hadoop.home.dir", "C:\\winutils");

			//			val conf = new SparkConf().setAppName("join").setMaster("local")
			val conf = new SparkConf().setAppName("join") 
					// Create a Scala Spark Context.
					val sc = new SparkContext(conf)

					val inputFile = args(0)
					val outputFile = args(1)

					val split_Rec = sc.textFile(inputFile)

					val map_data = split_Rec
					.map (line => line.split(",")
					.map(_.trim()).mkString(",") 
							).saveAsTextFile(outputFile)
	}
}