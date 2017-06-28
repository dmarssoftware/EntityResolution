package com.rs.GoogleAddrSegment

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json._

object GoogleAddressSegmentation {
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Google_Addr_Segment")
			val sc = new SparkContext(conf)

			val input = args(0)
			val outputFile = args(1)
			val apiKey = "AIzaSyBX1qKTponANRRRUUgVUBpGJpuKzbHeWPI" //bigkey
//			val apiKey = "AIzaSyCHc42hAbBwNz6z2h7ecuIduSZQMkxvR9Q"
			val full_seg = sc.textFile(input).map ( x => getProperAddress(x, apiKey) ).saveAsTextFile(outputFile)
	}
	
	def getProperAddress(dataString:String,key:String):String={
			val data = dataString.split(",")
					val place_id = data(2)
	
					if(place_id != ""){
						val jsonResult = googleAddrSegment(place_id, key)
								var output : JSONObject = null
//								println(place_id)
								output = new JSONObject(jsonResult).getJSONObject("result")
								println(output)
								
								val docs: JSONArray = output.getJSONArray("address_components")

								var segment_addr = ""
								var san_addr1 = ""
								var san_addr2 = ""
								var san_city = ""
								var san_state = ""
								var san_zip = ""
								var san_country = ""
								var csv = ""

								for (k <- 0 until data.length){
									if(k != 2 && k != 4){
										csv = csv + data(k) + ","
									}
								}

								for(i <- 0 until docs.length){
									if(docs.getJSONObject(i).getString("types").contains("locality")){
										san_city = docs.getJSONObject(i).getString("long_name").replaceAll(",", "~")
									}
									else if(docs.getJSONObject(i).getString("types").contains("administrative_area_level_1")){
										san_state = docs.getJSONObject(i).getString("long_name").replaceAll(",", "~")
									}
									else if(docs.getJSONObject(i).getString("types").contains("country")){
										san_country = docs.getJSONObject(i).getString("long_name").replaceAll(",", "~")
									}
									else if(docs.getJSONObject(i).getString("types").contains("postal_code")){
										san_zip = san_zip + docs.getJSONObject(i).getString("long_name").replaceAll(",", "~")
									}
									else if(docs.getJSONObject(i).getString("types").contains("neighborhood")){
										san_addr2 = docs.getJSONObject(i).getString("long_name").replaceAll(",", "~")
									}
									else if(!docs.getJSONObject(i).getString("types").contains("administrative_area_level_2")){
										san_addr1 = san_addr1 + docs.getJSONObject(i).getString("long_name").replaceAll(",", "~") + "~"
									}
									if(san_zip.length > 5){
									  san_zip = san_zip.substring(0, 5)
									}
									segment_addr = san_addr1 + "," + san_addr2 + "," + san_city + "," + san_state + "," + san_country + "," + san_zip
								}
								//	  		  println(docs.length())
								segment_addr + "," + csv.substring(0, csv.length - 1)
								
					}
					else{
						",,,,"+dataString						
					}
	}

	def googleAddrSegment(query:String,key:String):String={
			val url = "https://maps.googleapis.com/maps/api/place/details/json?placeid="+query+"&key="+key
					val result = scala.io.Source.fromURL(url).mkString
					println(url)
					result
	}
}