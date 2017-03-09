package com.rs.Deduplication

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

case class dedup_input_schema(rank:String, google_id: String, google_place_id:String, google_name: String, google_address:String, surr_key:String, id: String, bus_nm:String, addr_line1:String, city:String, state:String, zip5:String,zip4:String,country:String,comments:String,confidence:String)

/*
 * rank = 1
 * google_id = 2
 * google_place_id = 3
 * google_name = 4
 * google_address = 5
 * surr_key = 6
 * id = 7
 * bus_nm = 8
 * addr_line1 = 9
 * city = 10
 * state = 11
 * zip5 = 12
 * zip4 = 13
 * country = 14
 * comments = 15
 * confidence = 16
 */

object UniqueIdGenerate {
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Deduplication")
			val sc = new SparkContext(conf)
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      import sqlContext._
      import sqlContext.implicits._

      val inputFile = args(0)
      val outputFile = args(1)
      val reduceTasks = args(2)
      val delimitter = args(3)
      
			val dedup_in_file = sc.textFile(inputFile).filter( !_.isEmpty() )
			
			//Rank, Google_id, Google_name, Google_Address, Surrogate key, id, Business Name, Address Line 1, City, State, Zip5,zip4, Country, Comments
      val df= dedup_in_file.map( x => x.split(delimitter) ).map( x=> dedup_input_schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15))).toDF()

      //separate out null values
      val blank_df=df.filter(df("google_id")==="")
      
      //deduplicate only for values where google_id is not null
      val google_df = df.filter(df("google_id")!=="").dropDuplicates(Array("google_id"))

      //merge all values
      val merge_df = google_df.unionAll(blank_df).rdd.coalesce(reduceTasks.toInt).saveAsTextFile(outputFile)
  
	}
}