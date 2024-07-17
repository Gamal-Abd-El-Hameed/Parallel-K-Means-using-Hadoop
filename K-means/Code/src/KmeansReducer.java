import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for Kmeans algorithm
 */
public class KmeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	/**
	 * Reduce method to calculate the new centroids
	 * @param key The index of the centroid
	 * @param values The list of data points that belong to the centroid
	 * @param context Context of the job
	 * @throws IOException 
	 * @throws InterruptedException
	 */
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
    	// list that will contain the all points
    	List<double[]> CombinedPoints = new ArrayList<double[]>();
    	
    	
    	// iterate through the list of points and add them to the list
    	for(Text pointText: values){
    		double[] pointDouble = UtilitiesClass.stringToArray(pointText.toString(),
    				",");
    		CombinedPoints.add(pointDouble);
    	}
		// calculate the new centroid
    	double[] newCentroid = new double[CombinedPoints.get(0).length];
    	// sum
		for(double[] point: CombinedPoints){
    		for(int i = 0; i < point.length; i++){
    			newCentroid[i] += point[i];
    		}
    	}
    	// divide by the number of points
    	for(int i = 0; i < newCentroid.length; i ++){
    		newCentroid[i] /= CombinedPoints.size();
    	}
		// write the new centroid to the context
        context.write(key,new Text(UtilitiesClass.arrayToString(newCentroid, ",")));
//        ,new Text(sb.toString()));
    }

}