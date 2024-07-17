import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for Kmeans algorithm
 */
public class KmeansMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	private List<double[]> centroids; // List of centroids

	
    /**
     * Setup method to get the centroids from the configuration
     * @param context Context of the job
     */
	@Override
	protected void setup(Context context) {
        // Get the configuration
		Configuration conf = context.getConfiguration();
        // Get the centroids from the configuration
        centroids = UtilitiesClass.getCentroidsfromString(conf.get("centroids"));
        
	}

    /**
     * 
     * @param key Line number from which the mapper reads from the input file
     * @param value Line from the input file
     * @param context Context of the job
     * @throws IOException 
     * @throws InterruptedException
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        // System.out.println(value.toString());
    	// Convert the line to a double array
        double[] dataPoint = UtilitiesClass.stringToArray(value.toString(), ",");
        
        // Find the nearest centroid for the data point
        double minDistance = Double.MAX_VALUE;
        int nearestCentroidIndex = -1;
        
        // Find the nearest centroid
        for (int i = 0; i < centroids.size(); i++) {
            double[] centroid = centroids.get(i);
            double distance = UtilitiesClass.calculateDistance(dataPoint, centroid);
            if (distance < minDistance) {
                minDistance = distance;
                nearestCentroidIndex = i;
            }
        }
        // Write the nearest centroid index and the data point to the context
        String s = UtilitiesClass.arrayToString(dataPoint, ",");
        if (nearestCentroidIndex != -1) {
            context.write(new IntWritable(nearestCentroidIndex), new Text(s)) ;
        }
    }
    
}