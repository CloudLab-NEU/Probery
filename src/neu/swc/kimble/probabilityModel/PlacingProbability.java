package neu.swc.kimble.probabilityModel;

public class PlacingProbability {

	private final int bucketNumberPerGroup = 40;
	private final double[] probabilityArray = {0.01,0.02,0.03,0.04,0.06,0.07,0.08,0.09,0.10,0.11,0.12,0.13,0.14};
											/*{0.001,0.002,0.003,0.004,0.005,0.006,0.007,0.008,0.009,0.010,
											   0.011,0.012,0.013,0.014,0.015,0.016,0.017,0.018,0.019,0.020,
									           0.021,0.022,0.023,0.024,0.025,0.026,0.027,0.028,0.029,0.030,
									           0.031,0.032,0.033,0.034,0.035,0.036,0.037,0.038,0.039,0.040,
									           0.041,0.042,0.043,0.054}*/
	
	private static PlacingProbability  placingProbability = new PlacingProbability();
	private PlacingProbability(){
		
	}
	
	public static PlacingProbability getInstance(){
		return placingProbability;
	}
	
	public double[] getPlacingProbability(int blockNumber){
		int blockNumberInGroup = blockNumber % this.bucketNumberPerGroup;
		double[] placingProbability = new double[this.probabilityArray.length];
		if(blockNumberInGroup%2  == 0)
		{
			for(int i = 0; i < this.probabilityArray.length; i++)
			{
				placingProbability[i] = this.probabilityArray[this.probabilityArray.length -1 - i];
			}
		}
		else
			placingProbability = this.probabilityArray;
		return placingProbability;
	}	
	
	public int getBucketNumberPerGroup(){
		return this.bucketNumberPerGroup;
	}
}
