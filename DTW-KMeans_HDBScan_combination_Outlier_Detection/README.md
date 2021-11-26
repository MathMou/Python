# Outlier Detection: HDBScan and single cluster DTW-KMeans
Using a combination of HDBScan and DTW-KMeans to create a upper tier of outlier detection on data with much variance.
HDBScan is firstly used to limit the dataset to a high upper percentile to facilitate the use of DTW-KMeans with a single cluster.
DTW-KMeans with a single cluster and a distance metric hereby differentiates between datapoints of similar character close to the KMeans center and datapoints regarded as different than center-mean. 

HDBScan is used to limited the memory usage usually found in the SKLearn DBScan algorithme as it computes the full O(n^2) distance matrix.
