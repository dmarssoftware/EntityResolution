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
			val total_cols = args(11).toInt
			
			val preProcess = temp+"/preprocess"
			val checkFields = temp+"/checkFields"
			
			val mandFields = new ArrayBuffer[Int]()
			var columns = ""
			val nonMandFields = new ArrayBuffer[Int]()
			
			mandFields += addressline1
			mandFields += addressline2
			mandFields += businessName
			mandFields += zip
			mandFields += city
			mandFields += state
			mandFields += country
			
			for(i <- 0 until total_cols){
			  if(!mandFields.contains(i)){
			    nonMandFields += i
			  }
			}
			if(nonMandFields.length > 0){
			  for(i <- 1 until nonMandFields.length){
			    columns = columns + "line("+i+")+delimitter+"
			  }
			}
			println(columns)
			
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
			
			/*var unusedCol = ""
			var unusedFields = ""
			for( k <- 0 until unusedColumnArray.length){
				unusedCol = input.split("\n")(i).split(delimitter)(unusedColumnArray(k)).toUpperCase()
						unusedCol = unusedCol + delimitter
						unusedFields = unusedFields + unusedCol
			}*/
			
			val filter_rows = sc.textFile(checkFields)
			                  .map(_.split(delimitter))
			                  .filter(_.last=="No Missing Fields")
			                  .map (line => line(0)+delimitter+
			                                line(1)+delimitter+
			                                line(businessName)+delimitter+
			                                line(addressline1).toUpperCase().replaceAll("PO BOX", "")+delimitter+
			                                line(addressline2).toUpperCase().replaceAll("PO BOX", "")+delimitter+
			                                line(city)+delimitter+
			                                line(state)+delimitter+
			                                line(zip)+delimitter+
			                                line(country)+delimitter+
			                                line(country+1))
			                  .saveAsTextFile(dataQuality)
			                  
	}
	
	/*def nonMandatory(manFields:Array[Int],numCols:Int):String={
	  
	  var columns = ""
	  val nonManFields = new ArrayBuffer[Int]()
	  
	  for(i <- 0 until numCols){
			  if(!manFields.contains(i)){
			    nonManFields += i
			  }
			}
			if(nonManFields.length > 0){
			  for(i <- 0 until nonManFields.length){
			    columns = columns + "+delimitter+line("+(i+1)+")"
			  }
			}
			columns
	}*/
}