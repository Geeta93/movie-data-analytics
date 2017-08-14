/**
 * @author Geethika Garikapati
 * Find the number of movies released between 1950 and 1960.
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

public class MoviesReleased {
	
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			
			
			String[] tokenizer = value.toString().split(",");
			System.out.println("Line is: " +value);
			
			Integer movieYear;

			
				movieYear = Integer.parseInt(tokenizer[2]);
				if (movieYear.intValue() > 1950 && movieYear.intValue() < 1960) {
					context.write(new Text("1950-1960"), new IntWritable(1));
				}
			
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException,InterruptedException {
			int noOfMoviesReleased = 0;
			
			for(IntWritable x: values)
			{
				noOfMoviesReleased++;
			}
			context.write(new Text("No.of movies released between 1950 and 1960 are: "), new IntWritable(noOfMoviesReleased));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"moviesReleased");
		
		job.setJarByClass(MoviesReleased.class);
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