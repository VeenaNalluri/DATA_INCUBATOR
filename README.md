# DATA_INCUBATOR


For the challenge I have taken the Air Quality dataset available online at Kaggle.com. The dataset is the survey conducted by Minneapolis for Air Quality. The study found that few organic compounds above inhalation benchmarks i.e HRV (Health Risk Value). I have taken this dataset and imported into Spark as dataframes. One good thing about Spark is we can do Batch processing as well as Stream Processing (real time data) by integrating it with Apache Kafka. I have done Data Cleaning and applied filters based on Organic compound and the inhalation results. 


I have chosen two organic compounds Methylene Chloride and Trichloroethene and plotted the graph with inflammation results greater than HRV. The X axis indicates the count of organic compound values greater than HRV and Y axis indicates the inflammation result values. I have plotted this using MatPlot libraries which can be imported into Spark. We have to convert the dataframes into Pandas and then plot by providing the details of the plot like the style, X axis, Y axis and Title of the graph.


In outputs you can see the results of describe function for both the organic compounds. Spark has a function called df.describe() which gives you the count, standard deviation, minimum, maximum, mean of a provided data frame. Data Analysis is easy using Spark.  
