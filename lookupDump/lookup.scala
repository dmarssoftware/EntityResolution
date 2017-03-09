package com.rs.lookupDump

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

case class golden_copy( google_rank:String, google_id: String, google_place_id:String, google_name:String, google_fmt_addr:String, surr_key:String, id:String, bus_name:String,addr_line1:String,city:String,state:String,zip5:String,zip4:String,country:String,missing:String,confidence:String)

case class match_in(id: String, firm_name:String,bus_name:String, addr_line1:String,city:String,state:String,zip5:String,zip4:String,surr_key:String,country:String,missing:String)

object lookup {
	def main(args: Array[String]): Unit = {

						val sparkConf = new SparkConf().setAppName("LookupDump")
						val sc = new SparkContext(sparkConf)
						val sqlContext = new org.apache.spark.sql.SQLContext(sc)
						val inputGoldenCopy = args(0)
						val inputAddrSegment = args(1)
						val outputFile = args(2)
						val reduceTasks = args(3).toInt
						val delimitter = args(4)

						import sqlContext._
						import sqlContext.implicits._

						//this is the output of matching and before de-duplication
						val golden_copy_file = sc.textFile(inputGoldenCopy)
						val golden_copy_df= golden_copy_file.map( x => x.split(delimitter) ).map( x=> golden_copy(x(0),x(1),x(2),x(3).toUpperCase(),x(4).toUpperCase(),x(5).toUpperCase(),x(6).toUpperCase(),x(7).toUpperCase(),x(8).toUpperCase(),x(9).toUpperCase(),x(10).toUpperCase(),x(11).toUpperCase(),x(12).toUpperCase(),x(13).toUpperCase(),x(14).toUpperCase(),x(15)) ).toDF()

						//this is the file after address standardization and before matching
						val match_in_file = sc.textFile(inputAddrSegment)
						val match_df= match_in_file.map( x => x.split(delimitter) ).map( x=> match_in(x(0),x(1).toUpperCase(),x(2).toUpperCase(),x(3).toUpperCase(),x(4).toUpperCase(),x(5).toUpperCase(),x(6).toUpperCase(),x(7).toUpperCase(),x(8).toUpperCase(),x(9).toUpperCase(),x(10).toUpperCase()) ).toDF()


						golden_copy_df.registerTempTable("dedup")
						match_df.registerTempTable("match")

						//								sqlContext.sql("select d.google_id,d.google_name,m.surr_key,m.firm_name,m.addr_line1,m.addr_line2,m.city,m.state,m.zip,m.country from match m left outer join dedup d on m.firm_name=d.firm_name or m.firm_name = d.google_name").show()

						sqlContext
						.sql("select d.google_rank, d.google_id, d.google_place_id, d.google_name, d.google_fmt_addr, m.surr_key,m.id,m.bus_name,m.firm_name,m.addr_line1,m.city,m.state,m.zip5,m.zip4,m.country,m.missing from match m left outer join dedup d on (m.firm_name=UPPER(d.google_name) and m.addr_line1=d.addr_line1 and m.city = d.city and m.state = d.state and m.zip5 = d.zip5 and m.zip4 = d.zip4) OR (m.bus_name=d.bus_name and m.addr_line1=d.addr_line1 and m.city = d.city and m.state = d.state and m.zip5 = d.zip5 and m.zip4 = d.zip4 and d.google_id <> '')")
						.rdd
						.coalesce(reduceTasks.toInt)saveAsTextFile(outputFile)

						//after this call google places only for records which have google_id as null

				} 
	}