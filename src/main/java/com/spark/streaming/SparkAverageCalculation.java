package com.spark.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkAverageCalculation {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		Logger.getRootLogger().setLevel(Level.ERROR);
		
		JavaSparkContext sc = new JavaSparkContext(conf);
	    //inputList
	    List<Tuple2<String,Integer>> inputList = new ArrayList<Tuple2<String,Integer>>();
	    inputList.add(new Tuple2<String,Integer>("a1", 30));
	    inputList.add(new Tuple2<String,Integer>("b1", 30));
	    inputList.add(new Tuple2<String,Integer>("a1", 40));
	    inputList.add(new Tuple2<String,Integer>("a1", 20));
	    inputList.add(new Tuple2<String,Integer>("b1", 50));            
	    //parallelizePairs    
	    JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(inputList);
	    
	    JavaPairRDD<String, Tuple2<Integer, Integer>> valueCount = pairRDD.mapValues(value -> new Tuple2<Integer, Integer>(value,1));
	    JavaPairRDD<String, Tuple2<Integer, Integer>> reduceCount = valueCount.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));
	    JavaPairRDD<String, Integer> averagePair = reduceCount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Integer,Integer>>, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				Tuple2<Integer, Integer> tup = tuple._2;
				int total = tup._1;
				int count = tup._2;
				return new Tuple2<String, Integer>(tuple._1, total/count);
			}
		});
	    
	    averagePair.foreach(data -> {
	        System.out.println("Key="+data._1() + " Average=" + data._2());
	    });
	    
	    sc.stop();
	    sc.close();
	}
}
