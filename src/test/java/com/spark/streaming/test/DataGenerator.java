package com.spark.streaming.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DataGenerator {

	public static void main(String[] args) throws InterruptedException{
		File dir = new File("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshDataTest");
		File[] directoryListing = dir.listFiles();
		
		if (directoryListing != null) {
			int count = 1;
			for (File child : directoryListing) {
				System.out.println("File name :" + child.getName());
				
				/*FileReader fr = new FileReader(child);
				BufferedReader br = new BufferedReader(fr);
				FileWriter fileWriter = null;
		        BufferedWriter bufferedWriter = null;
				String line = null;
				
				while( (line = br.readLine())!= null){
					File file = new File("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshDataTestDest/file_" + count +".json");
					fileWriter = new FileWriter(file);
					fileWriter.write(line);
					fileWriter.flush();
				}
				fileWriter.close();
				count++;
				Thread.sleep(10000);*/
				BufferedWriter output = null;
				BufferedReader reader = null;
				try{
					File file = new File("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshDataTestDest/file_" + count +".json");
					reader = new BufferedReader(new FileReader(child));
					String line =null;
					while( (line = reader.readLine()) != null){
						System.out.println(line);
						output = new BufferedWriter(new FileWriter(file));
						output.append(line);
						output.flush();
					}
				}
				catch(Exception e){
				}
				finally{
					try {
						output.close();
						reader.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				count++;
				Thread.sleep(10000);
			}
		}
	}

}