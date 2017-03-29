package com.rs.Preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable._

object dataQuality {
	def main(args: Array[String]): Unit = {

	  val conf = new SparkConf().setAppName("PreProcessing")
			val sc = new SparkContext(conf)
			
			val inputFile = args(0)
			val temp = args(1)
			val dataQuality = args(2)
			val delimitter = args(3)
			val addressline1 = args(4).toInt
			val addressline2 = args(5).toInt
			val businessName = args(6).toInt
			val zip = args(7).toInt
			val city = args(8).toInt
			val state = args(9).toInt
			val country = args(10).toInt
//			val total_cols = args(11).toInt
			
			val preProcess = temp+"/preprocess"
			val checkFields = temp+"/checkFields"
			
			val mandFields = new ArrayBuffer[Int]()
			
			mandFields += businessName
			mandFields += addressline1
			mandFields += addressline2
			mandFields += city
			mandFields += state
			mandFields += zip
			mandFields += country
		
//			println(mandFields)
	
			val checkField = sc.textFile(inputFile)
			                .map (line => line.split(delimitter)
			                .map (_.replaceAll("[^a-zA-Z0-9 `~!@#$%^&*()-_=+{}|;:<>?,.\\/\\\'\\\"\\\\]", "")
//			                .map (_.replaceAll("[^a-zA-Z0-9 `~!@#$%^&*()-_=+{}|;:<>?,.\\/\\\'\\\"\\\\]", "")
			                		.trim()))
			                .map (field => field.mkString(delimitter))
			                .saveAsTextFile(preProcess)
			
			val data = sc.textFile(preProcess).map (
			    
			    line => if(line.split(delimitter)(addressline1) == line.split(delimitter)(addressline2)){
			      line.mkString+delimitter+"address line1 and address line 2 are same"
			    }
//			    else if(line.split(delimitter)(businessName).trim() == "" && line.split(delimitter)(addressline1).trim() == "" && line.split(delimitter)(addressline2).trim() == ""){
//			      line.mkString+delimitter+"Business name and address should be present"
//			    }
			    else if(line.split(delimitter)(businessName).trim() == ""){
			    	line.mkString+delimitter+"Business name should be present"
			    }
			    else if(line.split(delimitter)(zip).matches("^[0]*$")){
			      line.mkString+delimitter+"zip cannot be defaulted to 0"
			    }
			    else if(line.split(delimitter)(city).matches("[^a-zA-Z\\+]")){
			      line.mkString+delimitter+"city name should be populated with albhabets"
			    }
			    else if(line.split(delimitter)(businessName).matches("^[0-9]*$")){
			      line.mkString+delimitter+"Numbers found in Business Name"
			    }
			    else if(line.split(delimitter)(city).equalsIgnoreCase("USA")){
			      line.mkString+delimitter+"city name cannot have country populated"
			    }
			    else if(line.split(delimitter)(city).matches("^[0-9]*$")){
			      line.mkString+delimitter+"city name cannot contain numbers and zip codes"
			    }
			    else if(line.split(delimitter)(businessName).contains("www.") || line.split(delimitter)(businessName).contains("WWW.") ||
			            line.split(delimitter)(businessName).contains(".com") || line.split(delimitter)(businessName).contains(".COM")){
			      line.mkString+delimitter+"business name cannot contain urls"
			    }
 			    else{
			      line.mkString+delimitter+"No Missing Fields"
			    }
			).saveAsTextFile(checkFields)

			
			val filter_rows = sc.textFile(checkFields)
			                  .map(line => nonMandatory(line, mandFields))
//			                  .foreach(println)
			                  .saveAsTextFile(dataQuality)
			                  
	}
	
	def nonMandatory(data:String, manFields:ArrayBuffer[Int]):String={

	  val value = data.split(",")
	  val numCols = value.length
	  
	  var non_man_columns = ""
	  var man_columns = ""
	  val nonManFields = new ArrayBuffer[Int]()
	  
	  for(i <- 0 until numCols){
			  if(!manFields.contains(i)){
			    nonManFields += i
			  }
			}
			if(nonManFields.length > 0){
			  for(i <- 1 until nonManFields.length){
			    non_man_columns = non_man_columns + value(nonManFields(i)) + ","
			  }
			}
			for(j <- 0 until manFields.length){
			  man_columns = man_columns + value(manFields(j)) + ","
			}
			value(0)+","+man_columns+non_man_columns.substring(0,non_man_columns.length - 1)
	}
}