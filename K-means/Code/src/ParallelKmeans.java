import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ParallelKmeans {


	public static void main(String[] args) throws Exception{
        // Check if the number of arguments is correct
        if (args.length != 5) {
            System.err.println("Usage: ParallelKmeans <input path in HDFS>"
            		+ " <output path in HDFs>" + "<output path in local>"
            		+ " <number of clusters> <max iterations>");
            System.exit(1);
        }
        
        String inputPath = args[0]; // Input path in HDFS
        String outputPath = args[1]; // Output path in HDFS
        String outputPathLocal = args[2]; // Output path in local
        int numClusters = Integer.parseInt(args[3]); // Number of clusters
        int maxIterations = Integer.parseInt(args[4]); // Maximum number of iterations

        int count = 0; // Counter for the number of iterations
        List<String> Dataset = readDatasetFromFile(inputPath); // Read the dataset from the input path
        String centroids = InitializeRandomCentroids(Dataset,numClusters);
        long startTime = System.nanoTime();

        // Loop until the maximum number of iterations is reached
        while(count < maxIterations){

            // Print the iteration number
        	System.out.println("====================================================");
        	System.out.println("Iteration " + Integer.toString(count+1));
        	System.out.println("====================================================");
            Configuration conf = new Configuration();

          	 // Create a FileSystem object
            FileSystem fs = FileSystem.get(conf);

            // Delete the file or directory (if it exists)
            boolean deleted = fs.delete(new Path(outputPath), true);
            if(!deleted)
            	System.out.println("no file to delete");
            
            
            conf.setInt("numClusters", numClusters); // Set the number of clusters
            conf.set("centroids" ,centroids); // set the centroids
            
            
            //Instantiate a Job object and start clustering job
            Job job = Job.getInstance(conf, "parallel k-means");
            job.setInputFormatClass(TextInputFormat.class);
            job.setJarByClass(ParallelKmeans.class);
            job.setMapperClass(KmeansMapper.class);
            job.setReducerClass(KmeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            
            // wait for completion
            if (!job.waitForCompletion(true)) {
            	System.out.println("ERROR OCCURED WITH JOBS");
                System.exit(1);
            }
            count++; //increment the counter
            String newCentroids = readCentroidsFromFile(outputPath+"part-r-00000");
            System.out.println(newCentroids);
            List<double[]> newCentroidsDouble = UtilitiesClass.getCentroidsfromString(newCentroids); //get new centroids
            List<double[]> centroidsDouble = UtilitiesClass.getCentroidsfromString(centroids); // get old centroids
            int distances = 0;
            
            // calculate the distance between them
            for(int i = 0; i < newCentroidsDouble.size();i++){
            	distances+= UtilitiesClass.calculateDistance(centroidsDouble.get(i), newCentroidsDouble.get(i));
            }
            // copy the new centroids
            centroids = new String(newCentroids);

            //if very close then it converged
            if(distances < 0.001){
            	System.out.println("----------------------------------------------------");
            	System.out.println("Converged");
            	System.out.println("----------------------------------------------------");
            	break;
            }
            
        }
        long endTime = System.nanoTime();
        long executionTime = (endTime - startTime) / 1000000000; // Convert to seconds
        System.out.println("Execution time: " + executionTime + " seconds");
        // Assign each row in the dataset to its nearest centroid
        List<double[]> centroidsDouble = UtilitiesClass.getCentroidsfromString(centroids);
        for(int i = 0; i < Dataset.size(); i++){
        	double[] dataPoint = UtilitiesClass.stringToArray(Dataset.get(i), ",");
        	double minDistance = Double.MAX_VALUE;
        	int nearestCentroidIndex = -1;   
        	//calculate the nearest centroid
            for (int j = 0; j < centroidsDouble.size(); j++) {
                double[] centroid = centroidsDouble.get(j);
                double distance = UtilitiesClass.calculateDistance(dataPoint, centroid);
                if (distance < minDistance) {
                   	minDistance = distance;
                   	nearestCentroidIndex = j;
               	}
           	}
           	Dataset.set(i, Dataset.get(i) + ","+Integer.toString(nearestCentroidIndex));
        }
        // Write the dataset to the output path in local
        try{
        	FileWriter fileWriter = new FileWriter(outputPathLocal+"datasetClustered.data");
        	
        	BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        	
        	for(String row: Dataset){
        		bufferedWriter.write(row+"\n");
        	}
        	bufferedWriter.close();
        	fileWriter = new FileWriter(outputPathLocal+"Centroids.data");
        	
        	bufferedWriter = new BufferedWriter(fileWriter);
        	for(int i = 0; i < centroidsDouble.size(); i++){
        		bufferedWriter.write(UtilitiesClass.arrayToString(centroidsDouble.get(i),
            			",")+"\n");
        	}
        	bufferedWriter.close();

        	
        }
        catch(IOException e){
        	e.printStackTrace();
        }
        System.exit(0); // Exit the program
	}
	
	
    /**
     * Read the centroids from the output pathn
     * @param inputPath The path to the centroids file in the HDFS
     * @return The centroids as a string
     * @throws IOException
     */
	 private static String readCentroidsFromFile(String inputPath) throws IOException {
		 Configuration conf = new Configuration();
         FileSystem fs = FileSystem.get(conf);
         Path dataPath = new Path(inputPath); // HDFS path to your data file
         
         // Open the data file for reading
         BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dataPath)));
         String line = reader.readLine();
         StringBuilder sb = new StringBuilder();
         while(line != null && !line.isEmpty()){
        	 sb.append(line+"\n");
        	 line = reader.readLine();
         }
         return sb.toString();
	 }
	 
     /**
      * Read the dataset from the input path
      * @param inputPath The path to the dataset file in the HDFS
      * @return The dataset as a list of strings
      * @throws IOException
      */
	 private static List<String> readDatasetFromFile(String inputPath)throws IOException{
		 Configuration conf = new Configuration();
         FileSystem fs = FileSystem.get(conf);
         Path dataPath = new Path(inputPath); // HDFS path to your data file
         
         // Open the data file for reading
         BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(dataPath)));
         List<String> DatasetStrings = new ArrayList<String>();
         String line;
         while ((line = reader.readLine()) != null && !line.isEmpty()) {
             String[]parts = line.split(",");
             StringBuilder row = new StringBuilder();
             for(int i = 0 ; i < parts.length; i ++){
            	 try{
            		 Double.parseDouble(parts[i]);
            		 row.append(parts[i]);
            		 row.append(",");
            	 }
            	 catch(Exception e){}
             }
             row.deleteCharAt(row.length()-1);
             DatasetStrings.add(row.toString());
         }
         return DatasetStrings;
	 }
	 
	 
	 /**
      * Initialize the centroids randomly by sampling from the dataset
      * @param dataset The dataset
      * @param numClusters The number of clusters
      * @return The centroids as a string
      * @throws IOException
      */
	 private static String InitializeRandomCentroids(List<String> dataset,int numClusters)
			 throws IOException  {
		 		StringBuilder centroids = new StringBuilder();
	        	Random random = new Random();

		        // Generate random number within the range
		 		for(int i = 0; i < numClusters; i++){
		 			centroids.append(Integer.toString(i)+"\t");
			        int randomNumber = random.nextInt((dataset.size()) - 2);
			        
			        centroids.append(dataset.get(randomNumber)+ "\n") ;
		 		}
		 		return centroids.toString();
	 }

}
