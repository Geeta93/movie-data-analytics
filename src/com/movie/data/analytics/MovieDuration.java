/**
 * @author Geethika Garikapati
 * Find the number of movies with duration more than 2 hours (7200 second).
 */

package com.movie.data.analytics;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

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

public class MovieDuration {
	
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{

		public void map(LongWritable key, Text value,
				Context context)
				throws IOException,InterruptedException {
			
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), ",");
			
			while(stringTokenizer.hasMoreElements()) {
				String id = stringTokenizer.nextElement().toString();
				String movie = stringTokenizer.nextElement().toString();
				String year = stringTokenizer.nextElement().toString();
				String rating = stringTokenizer.nextElement().toString();
				
				try {
					String hours = stringTokenizer.nextElement().toString();
					Integer movieHours;
					if(!hours.isEmpty()) {
						movieHours = Integer.parseInt(hours);
						if(movieHours > 7200) {
							context.write(new Text("Hours more than 2"), new IntWritable(1));
						}
					}
				}  catch(ArrayIndexOutOfBoundsException e) {
					continue;
				}	catch(NoSuchElementException e) {
					continue;
				}
				
			}
			
		}
		
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context)
				throws IOException,InterruptedException {
			int totalMovieRating = 0;
			
			for(IntWritable x: values) {
				totalMovieRating++;
			}
			context.write(new Text("No.of movies with duration more than 2 hours: "), new IntWritable(totalMovieRating));
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
	
		Configuration conf= new Configuration();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"movieDuration");
		
		job.setJarByClass(MovieDuration.class);
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
