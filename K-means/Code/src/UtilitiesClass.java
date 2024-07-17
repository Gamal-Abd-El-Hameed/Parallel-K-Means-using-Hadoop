
import java.util.*;

/**
 * Class with utility methods used across the program
 */
public class UtilitiesClass {

	/**
	 * Method to convert a string to a double array
	 * @param str The string to convert
	 * @param delimiter The delimiter to use
	 * @return The double array
	 */
	public static double[] stringToArray(String str, String delimiter) {
        String[] parts = str.split(delimiter);
        ArrayList<Double> arr = new ArrayList<Double>();
        for (int i = 0; i < parts.length; i++) {
        	// Try to parse the string to a double
			try{
             arr.add(Double.parseDouble(parts[i])); // Add the double to the array
        	}
        	catch(NumberFormatException e){        
        		
        	}
        }
        // Convert ArrayList to double array
        double[] doubleArray = new double[arr.size()];
        for (int i = 0; i < arr.size(); i++) {
            doubleArray[i] = arr.get(i);
        }
        return  doubleArray;
    }

	/**
	 * Method to convert a double array to a string
	 * @param arr The double array to convert
	 * @param delimiter The delimiter to use
	 * @return the array as a string 
	 */
	public static String arrayToString(double[] arr, String delimiter) {
	        StringBuilder sb = new StringBuilder();
	        for (int i = 0; i < arr.length; i++) {
	            sb.append(arr[i]);
	            if (i < arr.length - 1) {
	                sb.append(delimiter);
	            }
	        }
	        return sb.toString();
	}
	
	/**
	 * Method to get the centroids from a string
	 * @param s The string to get the centroids from
	 * @return The centroids as a list of double arrays
	 */
	public static List<double[]> getCentroidsfromString(String s){
		String[] centroidsStrings = s.split("\n");
		List<double[]> centroidsDouble = new ArrayList<double[]>();
		for(String outer : centroidsStrings){
			String centroid = outer.split("\t")[1];
			String[] centroidFeaturesString = centroid.split(",");
			double[] centroidFeaturesDouble = new double[centroidFeaturesString.length];
			for(int i = 0; i < centroidFeaturesString.length; i ++){
				centroidFeaturesDouble[i] = Double.parseDouble(centroidFeaturesString[i]);
			}
			centroidsDouble.add(centroidFeaturesDouble);
		}
		return centroidsDouble;
	}
	

	/**
	 * Method to calculate the distance between two points
	 * @param point1 
	 * @param point2
	 * @return The distance between the two points
	 */
	public static double calculateDistance(double[] point1, double[] point2) {
        double sum = 0;
        for (int i = 0; i < point1.length; i++) {
            sum += Math.pow(point1[i] - point2[i], 2);
        }
        return Math.sqrt(sum);
    }

}
