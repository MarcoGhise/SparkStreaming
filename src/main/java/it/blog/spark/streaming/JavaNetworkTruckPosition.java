/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.blog.spark.streaming;

import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network
 * every second.
 *
 * Usage: JavaNetworkWordCount <hostname> <port> <hostname> and <port> describe
 * the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$
 * nc -lk 9999` and then run the example `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class JavaNetworkTruckPosition {
	/*
	 * Map report
	 */
	private static Map<String, Integer> truckMap = new HashMap<String, Integer>();
	
	final static Logger logger = Logger.getLogger(JavaNetworkTruckPosition.class);
	
	public static void main(String[] args) throws Exception {
		/*
		 * Setting the configuration with the minimun number of threads (2)
		 */
		SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkTruckPosition").setMaster("local[2]");
		/*
		 * Load the configuration
		 */
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		/*
		 * Open the socket connection
		 */
		JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost", 6789); // ,
		/*
		 * Receive every location in the format
		 * truck_number|long|lat
		 */
		JavaDStream<String> trucks = lines.map(x -> x.split("|")[0]);
		/*
		 * Grouping the data by trucks
		 */
		JavaPairDStream<String, Integer> truckCounts = trucks.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);
		/*
		 * Adding the new information bulk into the existing one
		 */
		VoidFunction<JavaPairRDD<String, Integer>> foreachFunc = truck -> {
			truck.foreach(record -> {
				
				if (truckMap.containsKey(record._1))
					truckMap.put(record._1, truckMap.get(record._1) +  record._2);
				else
					truckMap.put(record._1, record._2);
				
			});
			logger.info("***************** Truck Positions ***************");
			
			truckMap.forEach((key, value) -> logger.info(String.format("The truck %s has been recorded in %s position(s)", key, value)));
			
			logger.info("********************************");
		};

		truckCounts.foreachRDD(foreachFunc);
		/*
		 * Start the process
		 */
		ssc.start();
		/*
		 * Waiting for the end signal
		 */
		ssc.awaitTermination();
		ssc.close();
	}
}