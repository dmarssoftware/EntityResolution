package com.rs.Surrogate

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.commons.io.FilenameUtils;

object surrogateKey {
  def main(args: Array[String]): Unit = {
    
    val inputFile = args(0)
    val outputFile = args(1)
    val delimitter = args(2)
    val basename = FilenameUtils.getBaseName(inputFile)
    val extension = FilenameUtils.getExtension(inputFile)
    val fileName = basename+"."+extension
    
    val conf = new SparkConf().setAppName("Surrogate_Key")
    val sc = new SparkContext(conf)
    
    val sample1 = sc.textFile(inputFile)
    sample1.zipWithIndex().map(f => fileName+"_"+(f._2+1)+delimitter+f._1).saveAsTextFile(outputFile)
  }
}