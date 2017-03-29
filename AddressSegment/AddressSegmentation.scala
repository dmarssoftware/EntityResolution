package com.rs.AddressSegment

import scala.io.Source
import java.io._
import scala.xml._
import java.net.URLEncoder
import org.apache.spark._
import scala.collection.mutable.ArrayBuffer

object AddressSegmentation {
	def main(args: Array[String]): Unit = {
			
			val conf = new SparkConf().setAppName("Address Segment")
			val sc = new SparkContext(conf)

			val inputFile = args(0)
			val outputFile = args(1)
			val merchLookup = args(2)
			val delimitter = args(3)
			val businessName = args(4).toInt
			val address1 = args(5).toInt
			val address2 = args(6).toInt
			val city = args(7).toInt
			val state = args(8).toInt
			val zip5 = args(9).toInt
//			val zip4 = args(9)
      val country = args(10).toInt
      
      val lookupPath =sc.textFile(merchLookup).collect()
			val inputPath = sc.textFile(inputFile)

			val addr_segment = inputPath.map ( x => AddressStandard(x,delimitter,businessName,address1,address2,city,state,zip5,country,lookupPath) )

			addr_segment.saveAsTextFile(outputFile)
			

	}
		
def AddressStandard(input:String,delimitter:String,businessName:Int,address1:Int,address2:Int,city:Int,state:Int,zip5:Int,country:Int,merchLookup:Array[String]):String={


//					val property = Source.fromFile(configFile).mkString
					var unusedColumnArray = ArrayBuffer[Int]()
					var usedColumnArray = ArrayBuffer[Int]()
					var dataFields = ""
					var unusedFields = ""
					var unusedCol = ""

//					val properties = XML.loadString(property)
//					val firmName_index = (properties \ "FirmName").text
//					val address1_index = (properties \ "Address1").text
//					val address2_index = (properties \ "Address2").text
//					val city_index = (properties \ "City").text
//					val state_index = (properties \ "State").text
//					val zip5_index = (properties \ "Zip5").text
//					val zip4_index = (properties \ "Zip4").text

					usedColumnArray += businessName.toInt
					usedColumnArray += address1.toInt
					usedColumnArray += address2.toInt
					usedColumnArray += city.toInt
					usedColumnArray += state.toInt
					usedColumnArray += zip5.toInt
					usedColumnArray += country.toInt
//					usedColumnArray += zip4.toInt
//										println(usedColumnArray)

					var firmName_req = ""; var zip4_req = "";	var address1_req = ""; var address2_req = ""; var city_req = ""; var state_req = ""; var zip5_req = ""
					var country_req = ""; var comments_req = ""; var surrKey_req = ""

					for(j <- 0 until input.split("\n")(0).split(delimitter).length){
						if(!usedColumnArray.contains(j)){
							unusedColumnArray += j
						}
					}
//					unusedColumnArray += businessName.toInt
//										println(unusedColumnArray)

					for(i <- 0 until input.split("\n").length){

						try{
							firmName_req = input.split("\n")(i).split(delimitter)(businessName.toInt).toUpperCase()
						}
						catch{
						case _: NumberFormatException => { firmName_req = "" }
						}

						try{
							address1_req = input.split("\n")(i).split(delimitter)(address1.toInt).toUpperCase()
						}catch{
						case _: NumberFormatException => { address1_req = "" }
						}

						try{
							address2_req = input.split("\n")(i).split(delimitter)(address2.toInt).toUpperCase()
						}catch{
						case _: NumberFormatException => { address2_req = "" }
						}

						try{
							city_req = input.split("\n")(i).split(delimitter)(city.toInt).toUpperCase()
						}catch{
						case _: NumberFormatException => { city_req = "" }
						}

						try {
							state_req = input.split("\n")(i).split(delimitter)(state.toInt).toUpperCase()  
						}catch{
						case _: NumberFormatException => { state_req = "" }
						}

						try{
							zip5_req = input.split("\n")(i).split(delimitter)(zip5.toInt).toUpperCase()
						}catch{
						case _: NumberFormatException => { zip5_req = "" }
						}

						country_req = input.split("\n")(i).split(delimitter)(country.toInt).toUpperCase()
//						comments_req = input.split("\n")(i).split(delimitter)(country.toInt+1)
						surrKey_req = input.split("\n")(i).split(delimitter)(0).toUpperCase()
						
//						try{
//							zip4_req = input.split("\n")(i).split(delimitter)(zip4.toInt)
//						}catch{
//						case _: NumberFormatException => { zip4_req = "" }
//						}


						val apiUrl = "http://production.shippingapis.com/ShippingAPI.dll?API=Verify&XML="
						val xmlReqUrl = <AddressValidateRequest USERID="997RSSOF4114">
						<IncludeOptionalElements>true</IncludeOptionalElements>
						<ReturnCarrierRoute>true</ReturnCarrierRoute>
						<Address ID="0">  
						<FirmName>{firmName_req}</FirmName>   
						<Address1>{address1_req}</Address1>   
						<Address2>{address2_req}</Address2>   
						<City>{city_req}</City>   
						<State>{state_req}</State>   
						<Zip5>{zip5_req}</Zip5>   
						<Zip4>{zip4_req}</Zip4> 
						</Address>      
						</AddressValidateRequest>

						val xmlreqUrl = URLEncoder.encode(xmlReqUrl.toString(), "UTF-8")
								val reqUrl = apiUrl + xmlreqUrl

								val result = Source.fromURL(reqUrl).mkString

								val xmlString = XML.loadString(result)

								var firmName_data = (xmlString \ "Address" \ "FirmName").text.replaceAll("[~!-@#$%^&*()+={}:;]", "").replaceAll("\\d", "").replaceAll("\\bCE\\b", "").trim()
								var address_data = (xmlString \ "Address" \ "Address2").text.replaceAll("[~!@#$%^&*()+={}:;]", "").trim().replaceAll("^[0-9]*$", "").replaceAll("NULL", "")
								var city_data = (xmlString \ "Address" \ "City").text
								var state_data = (xmlString \ "Address" \ "State").text
								var zip5_data = (xmlString \ "Address" \ "Zip5").text
								var zip4_data = (xmlString \ "Address" \ "Zip4").text
//								val deliveryPoint_data = (xmlString \ "Address" \ "DeliveryPoint").text
//								val carrierRoute_data = (xmlString \ "Address" \ "CarrierRoute").text
								
								for(i<-0 until merchLookup.length){
								  if(firmName_data.matches(".*\\b"+merchLookup(i).toUpperCase()+"\\b.*") || firmName_req.matches(".*\\b"+merchLookup(i).toUpperCase()+"\\b.*")){
								      firmName_data = merchLookup(i).toUpperCase().replaceAll("[~!-@#$%^&*()+={}:;]", "")
								  }
								}
								
								if(firmName_data == ""){
									comments_req = "USPS Mismatch "
								}
								else{
									comments_req = input.split("\n")(i).split(delimitter)(input.split("\n")(0).split(delimitter).length -1)
								}
								
								if(firmName_data == ""){
								  firmName_data = input.split("\n")(i).split(delimitter)(businessName.toInt).toUpperCase().replaceAll("[~!-@#$%^&*()+={}:;]", "").replaceAll("\\d", "").trim()
								}
						    if(address_data == ""){
						      address_data = input.split("\n")(i).split(delimitter)(address1.toInt).toUpperCase().replaceAll("[~!@#$%^&*()+={}:;]", "").trim().replaceAll("^[0-9]*$", "").replaceAll("NULL", "")+
						                      input.split("\n")(i).split(delimitter)(address2.toInt).toUpperCase().replaceAll("[~!@#$%^&*()+={}:;]", "").trim().replaceAll("^[0-9]*$", "").replaceAll("NULL", "")
						    }
						    if(city_data == ""){
						      city_data = input.split("\n")(i).split(delimitter)(city.toInt).toUpperCase()
						    }
						    if(state_data == ""){
						      state_data = input.split("\n")(i).split(delimitter)(state.toInt).toUpperCase()
						    }
						    if(zip5_data == ""){
						      zip5_data = input.split("\n")(i).split(delimitter)(zip5.toInt)
						      if(zip5_data.length() > 5){
						        zip5_data = zip5_data.substring(0, 5)
						      }
						    }
						    if(zip5_data.length > 0){
						      zip4_data = ""
						    }
						    
								unusedCol = ""
								unusedFields = ""
								for( k <- 1 until unusedColumnArray.length - 1){
									unusedCol = input.split("\n")(i).split(delimitter)(unusedColumnArray(k))
											unusedCol = unusedCol + ";"
											unusedFields = unusedFields + unusedCol
								}

								val outputData = unusedFields.substring(0, unusedFields.length - 1)+delimitter+
								                  firmName_data+delimitter+
								                  firmName_req+delimitter+
								                  address_data+delimitter+
								                  city_data+delimitter+
								                  state_data+delimitter+
								                  zip5_data+delimitter+
								                  zip4_data+delimitter+
								                  surrKey_req+delimitter+
								                  country_req+delimitter+
								                  comments_req//deliveryPoint_data+delimitter+carrierRoute_data//+unusedCol
								dataFields = dataFields + outputData
					}
					dataFields
	}
}