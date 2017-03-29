package com.rs.googlePlaces

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.json._
import org.apache.spark.rdd._

object placesCheck {
  //"[null,null,null,null,null,DBSOURCE.TXT_17,2202,LBJS GROCERY,LBJS GROCERY,44 WORCESTER VILLAGE RD,WORCESTER,VT,05682,,USA,NO MISSING FIELD]"

  var google_rank = 0
  var google_id = 1
  var google_place_id = 2
  var google_name = 3
  var google_addr = 4
  
  var surr_key = 5
  var id = 6
  var orig_business_name = 7
  var business_name = 8
  var address = 9
  var city = 10
  var state = 11
  var zip5 = 12
  var zip4 = 13
  var country = 14
  var comments = 15
	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("googlePlaces")
					val sc = new SparkContext(conf)

					//			val inputFile = "addr_segment/"
					val inputFile = args(0)
					//			val checkFields = "checkFields/"
					val checkFields = args(1)
					//			val delimitter = ","
					val delimitter = args(2)
					val apiKey = "AIzaSyBX1qKTponANRRRUUgVUBpGJpuKzbHeWPI"

					val gen_Id = sc.textFile(inputFile).filter( !_.isEmpty() )
					val PlaceMatch = gen_Id.map ( x => query_all(x,apiKey)).saveAsTextFile(checkFields)
	}

	// All fields in query	
  def query_all(data:String,key:String):String={
		  val field = data.split(",")
				  val confValue = "95% Confidence"

				  var queryFields = ""

				  for(j <- business_name until comments){
				    if(j != orig_business_name){
				    	queryFields = queryFields + field(j) + "+"
				    }
				  }
//		  queryFields = queryFields + field(country)

		  val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
		  println(query)

		  if(field(google_id).contains("null")){
			  val resultString = getJsonString(query,key,field,confValue)

					  if(resultString != ""){
						  resultString
					  }
					  else{
						  query_all_x_address(data,key)
					  }
		  }
		  else{
		    var untouched_data = ""
		    for(i<- 0 until field.length - 1){
		      if(i != business_name){
		    	  untouched_data = untouched_data + field(i) + ","
		      }
		    }
		    untouched_data + "No Missing Field,Matched from Lookup" 
//			  field(google_rank)+","+field(google_id)+","+field(google_name)+","+field(surr_key)+","+field(id)+","+field(orig_business_name)+","+field(address)+","+field(city)+","+field(state)+","+field(zip5)+","+field(zip4)+","+field(country)+",No Missing Field,Matched from Lookup"
		  }
  }

	// Remove Address Line
	def query_all_x_address(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "90% Confidence"
					var queryFields = ""
					for(j <- business_name until comments){
						if(j != address && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}
//			queryFields = queryFields + field(country)

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)
			if(resultString != ""){
			  resultString
			}
			else{
				query_all_x_zip_addr(data,key)
			}
	}


	//Remove zip4
	def query_all_x_zip4(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "90% Confidence"
					var queryFields = ""
					for(j <- business_name until comments){
						if(j != zip4 && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)
			if(resultString != ""){
				resultString
			}
			else{
				query_all_x_zip_addr(data, key)
			}
	}

	// Only business_name+city+state+country
	def query_all_x_zip_addr(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "90% Confidence"
					var queryFields = ""
					for(j <- business_name until comments){
						if(j != address && j != zip4 && j != zip5 && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}
//			queryFields = queryFields + field(country)

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)
			if(resultString != ""){
			  resultString
			}
			else{
				query_all_x_city_addr(data,key)
			}
	}


	// Only business_name+state+zip+country
	def query_all_x_city_addr(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "90% Confidence"
					var queryFields = ""
					for(j <- business_name until comments){
						if(j != address && j != city && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}
//			queryFields = queryFields + field(country)

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)
			if(resultString != ""){
			  resultString
			}
			else{
				query_all_x_zip_country(data,key)
			}
	}

	// Only business_name+address+city+state
	def query_all_x_zip_country(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "85% Confidence"
					var queryFields = ""
					for(j <- business_name until country){
						if(j != zip4 && j != zip5 && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
					println(query)
					val resultString = getJsonString(query,key,field,confValue)
					if(resultString != ""){
					  resultString
					}
					else{
						query_all_x_addr_zip_city(data, key)
					}
	}

	// Only business_name+state+country
	def query_all_x_addr_zip_city(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "75% Confidence"
					var queryFields = ""
					for(j <- business_name until comments){
						if(j != zip4 && j != zip5 && j != address && j != city && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}
//			queryFields = queryFields + field(country)

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)
			if(resultString != ""){
			  resultString
			}
			else{
				query_x_zip_city(data, key)
			}
	}


	//Only Business_name+address+state+country
	def query_x_zip_city(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "75% Confidence"
					var queryFields = ""
					for(j <- business_name until comments){
						if(j != zip4 && j != zip5 && j !=city && j != orig_business_name){
							queryFields = queryFields + field(j) + "+"
						}
					}
//			queryFields = queryFields + field(country)

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)
			if(resultString != ""){
			  resultString
			}
			else{
				query_bus_nm_country(data,key)
			}
	}

	
	// Only Business_Name+Country
	def query_bus_nm_country(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "70% Confidence"
					var queryFields = field(business_name) + "+" + field(country)

					val query = queryFields.replaceAll("\\s+","+")
					println(query)

					val resultString = getJsonString(query,key,field,confValue)
					if(resultString != ""){
					  resultString
					}
					else{
						query_all_orig_business(data,key)
					}
	}

	// Query with original business name
	def query_all_orig_business(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "95% Confidence"
					var queryFields = ""
					for(j <- orig_business_name until comments){
					  if( j != business_name){
						  queryFields = queryFields + field(j) + "+"
					  }
					}
//			queryFields = queryFields + field(country)

			val query = queryFields.substring(0, queryFields.length - 1).replaceAll("\\s+", "+")
			println(query)
			val resultString = getJsonString(query,key,field,confValue)

				if(resultString != ""){
					resultString
				}
				else{
					query_all_business_name(data, key)
				}
	}

	
	// Only Business_Name in the query
	def query_all_business_name(data:String,key:String):String={
			val field = data.split(",")
			val confValue = "60% Confidence"
			var queryFields = ""
			queryFields = queryFields + field(business_name)

			val query = queryFields.replaceAll("\\s+", "+")
			println(query)
			
			val resultString = getJsonString(query,key,field,confValue)

				if(resultString != ""){
					resultString
				}
				else{
					getNoValue(data)
				}
	}
	
		def getJsonString(queryString:String,key:String,fields:Array[String],confidenceValue:String):String={
			val jsonResult = placeMatch(queryString, key)
					var output : JSONObject = null
					output = new JSONObject(jsonResult)
					val docs: JSONArray = output.getJSONArray("results")

					var google_id = ""
					var google_name = ""
					var csv = ""
					var google_fmt_address = ""
					var google_place_id = ""
					var output_String = ""
					
					for (k <- surr_key to comments){
						if(k != business_name){
							csv = csv + fields(k) + ","
						}
					}

					if(docs.length() > 0 && docs.getJSONObject(0).getString("types").contains("establishment")){
						
					  for(i <- 0 until docs.length()){
							  google_id = docs.getJSONObject(i).getString("id")
									  google_name = docs.getJSONObject(i).getString("name").replaceAll(",", "~")
									  google_fmt_address = docs.getJSONObject(i).getString("formatted_address").replaceAll(",", "~")
									  google_place_id = docs.getJSONObject(i).getString("place_id")

									  output_String = output_String + (i+1) + "," + google_id + "," + google_place_id + "," + google_name + "," + google_fmt_address + "," + csv + confidenceValue + "\n" 								
						}
						println(docs.length()) // Number of Hits
						output_String.substring(0, output_String.length - 1)
					}
					else{
						""
					}
	}

	def getNoValue(data:String):String={
			val fields = data.split(",")
					var csv = ""

					for (k <- surr_key until comments){
						if(k != business_name){
							csv = csv + fields(k) + ","
						}
					}
			",,,,," + csv + "Google Place Mismatch" + ",Record not found in Universe"
	}

	def placeMatch(query:String,key:String):String={
			val url = "https://maps.googleapis.com/maps/api/place/textsearch/json?query="+query+"&key="+key
					val result = scala.io.Source.fromURL(url).mkString
					result  
	}
}