package com.spark.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import scala.Tuple2;

public class RsiSimulation {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
	    //inputList
	    List<Tuple2<String,Double>> inputList = new ArrayList<Tuple2<String,Double>>();
	    inputList.add(new Tuple2<String,Double>("a1",  Double.parseDouble("30.00")));
	    inputList.add(new Tuple2<String,Double>("a1", Double.parseDouble("-30.00")));
	    inputList.add(new Tuple2<String,Double>("a1", Double.parseDouble("40.00")));
	    inputList.add(new Tuple2<String,Double>("a1", Double.parseDouble("-20.00")));
	    inputList.add(new Tuple2<String,Double>("a1", Double.parseDouble("50.00")));            
	    //parallelizePairs    
	    JavaPairRDD<String, Double> stockProfitRDD = sc.parallelizePairs(inputList);
	    
	    //JavaPairRDD<String, Double> stockLosesRDD = stockProfitRDD.mapToPair(f -> new Tuple2(f._1, f._2));
	    
		JavaPairRDD<String, Double> gains = stockProfitRDD.filter(x -> x._2 > 0).reduceByKey((x, y) -> {return x+y;});
	    gains.foreach(f -> {System.out.println(f._1 + " - " + f._2 );});
	    
		JavaPairRDD<String, Double> losses = stockProfitRDD.filter(x -> x._2 < 0).reduceByKey((x, y) -> {return Math.abs(x)+Math.abs(y);});
	    losses.foreach(f -> {System.out.println(f._1 + " - " + f._2 );});
	}
}
