/**
 * @author Geethika Garikapati
 * Find the movies whose rating are between 3 and 4.
 */

package com.movie.data.analytics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MoviesBetweenRating {

	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			
			String[] tokenizer = value.toString().split(",");
			System.out.println("Line is: " +value);
			Double movieRating = 0.0;
			if (!tokenizer[3].isEmpty()) {
				movieRating = Double.parseDouble(tokenizer[3]);
				if (movieRating > 3.0 && movieRating < 4.0) {
					context.write(new Text("Movie rating between 3.0-4.0"), new IntWritable(1));
				}
			}
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException,InterruptedException {
			int totalMovieRating = 0;
			
			for(IntWritable x: values)
				totalMovieRating++;

			context.write(new Text("No.of movies with rating between 3.0 and 4.0 are: "), new IntWritable(totalMovieRating));
			//context.write(new Text("Total no.of movies with rating "+key+" are: "), new IntWritable(totalMovieRating));
		}
		
	}
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"movieRating");
		
		job.setJarByClass(MoviesBetweenRating.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outputPath = new Path(args[1]);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		outputPath.getFileSystem(conf).delete(outputPath);
			
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
