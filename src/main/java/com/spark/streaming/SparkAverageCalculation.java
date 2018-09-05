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
	    List<Tuple2<String,Double>> inputList = new ArrayList<Tuple2<String,Double>>();
	    inputList.add(new Tuple2<String,Double>("a1",  Double.parseDouble("30.00")));
	    inputList.add(new Tuple2<String,Double>("b1", Double.parseDouble("30.00")));
	    inputList.add(new Tuple2<String,Double>("a1", Double.parseDouble("40.00")));
	    inputList.add(new Tuple2<String,Double>("a1", Double.parseDouble("20.00")));
	    inputList.add(new Tuple2<String,Double>("b1", Double.parseDouble("50.00")));            
	    //parallelizePairs    
	    JavaPairRDD<String, Double> pairRDD = sc.parallelizePairs(inputList);
	    
	    JavaPairRDD<String, Tuple2<Double, Double>> valueCount = pairRDD.mapValues(value -> new Tuple2<Double, Double>(value,1.0));
	    JavaPairRDD<String, Tuple2<Double, Double>> reduceCount = valueCount.reduceByKey((x,y) -> new Tuple2<>(x._1 + y._1, x._2 + y._2));
	    JavaPairRDD<String, Double> averagePair = reduceCount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Double,Double>>, String, Double>() {

			@Override
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
				Tuple2<Double, Double> tup = tuple._2;
				Double total = tup._1;
				Double count = tup._2;
				return new Tuple2<String, Double>(tuple._1, total/count);
			}
		});
	    
	    averagePair.foreach(data -> {
	        System.out.println("Key="+data._1() + " Average=" + data._2());
	    });
	    
	    sc.stop();
	    sc.close();
	}
}
