# Weather
Weather Data Analysis
Gopinathan K. Munappy

1. Whole project try to do in "big data processing" fashion where there is large dataset is in question.

2. Procured input data from OpenWeatherMap, "http://bulk.openweathermap.org/sample/" as a raw CSV file. Records are more than 3.7 lacs and size is 30,783,916.

3. After analysing the data, decided to choose following platform.

   3.1 Apache Spark 1.4.1, Spark SQL 1.4.1
  
   3.2 Scala 2.10.4

   3.3 java version "1.8.0_77"

   3.4 OS: CentOS 6.4

   3.5 Apache Hadoop: Hadoop 2.5.0-cdh5.3.0

   3.6 SBT: 0.13.5

   3.6 Hadoop Distribution: Cloudera CDH 5.3.0 Quickstart VM


4. Project Artifacts:

   4.1 Preprocess.scala for pre-processing, cleaning and removing repetative records.

   4.2 Process.scala for:

         4.2.1 Loading cleaned data into Spark from local directory.
       
         4.2.2 Processing like removing header, removing BLANK records if any.

         4.2.3 Functions Used:
            
               1. converDate(): For converting unix timestamp to ISO8609 format
               2. roundTo2Decimal(): For converion of double to 2 decimal precission
               3. getDesc(): For getting actual weather condition from weather ID.

5. Data:

         5.1 Input raw data weather_details.csv, converted to TSV  as many fields containing commas, stored in {Work-Dir}/data/

         5.2 Intermediate data cleaned from above step moved to HDFS. Records were more than 75000, size 6,513,752.

         5.3 Final output emitted to {Work-Dir}/data/  as saveFile. Size: 7,325,140.

       
6. Project Structure:

   	6.1 Working Directory: /home/cloudera/spark/Weather/

	6.2 Source Directory: /{Work-Dir}/src/main/scala/Process.scala, Preprocess.scala

	6.3 Build file: {Work-Dir}/weather.sbt  

	6.4 Data: 
		 6.4.1: Input:  /{Work-Dir}/data/weather_details.txt

	         6.4.2: Intermediate: /{Work-Dir}/data/weather/preprocess.txt (in HDFS)

		 6.4.3: Final output: /{Work-Dir}/data/saveFile.txt/part-00000, _SUCCESS

Sample output emitted:

[root@quickstart Weather]# cd data
[root@quickstart data]# ls
preprocessed.txt  saveFile.txt  weather_details.txt
[root@quickstart data]# cd saveFile.txt
[root@quickstart saveFile.txt]# ls
part-00000  _SUCCESS
[root@quickstart saveFile.txt]# head -100 part-00000

Ajman Ajman United Arab Emirates|25.41111,55.43504|03-12-2014 20:00:00|+24.65|1020.66|72|Clear sky
Al Ain Abu Dhabi United Arab Emirates|24.20732,55.68615|03-12-2014 20:00:00|+22.0|982.89|40|Clear sky
Dubai Dubai United Arab Emirates|25.258169,55.304722|03-12-2014 20:00:00|+24.61|1020.66|72|Clear sky
Ras Al Khaimah Ras al Khaimah United Arab Emirates|25.80955,55.97211|03-12-2014 20:00:00|+26.0|999.34|100|Scattered clouds
Salta Salta Province Argentina|-24.7859,-65.411659|03-12-2014 20:00:00|+17.79|827.25|98|Broken clouds
Avellaneda Buenos Aires Province Argentina|-29.117611,-59.65834|03-12-2014 20:00:00|+18.36|1022.76|58|Clear sky
Bahia Blanca Buenos Aires Province Argentina|-38.719601,-62.27243|03-12-2014 20:00:00|+20.0|1025.03|99|Clear sky
Balcarce Buenos Aires Province Argentina|-37.846161,-58.255219|03-12-2014 20:00:00|+14.41|1017.5|84|Few clouds
Brandsen Buenos Aires Province Argentina|-35.168419,-58.234268|03-12-2014 20:00:00|+17.71|1027.63|72|Clear sky
Campana Buenos Aires Province Argentina|-34.168739,-58.959141|03-12-2014 20:00:00|+19.91|1028.36|59|Clear sky
Chacabuco Buenos Aires Province Argentina|-34.64167,-60.473888|03-12-2014 20:00:00|+18.51|1021.14|62|Scattered clouds
Del Viso Buenos Aires Province Argentina|-34.45013,-58.787811|03-12-2014 20:00:00|+19.91|1028.36|59|Clear sky
Isidro Casanova Buenos Aires Province Argentina|-34.699329,-58.590408|03-12-2014 20:00:00|+22.6|1026.82|61|Clear sky
Junin Buenos Aires Province Argentina|-34.58382,-60.943321|03-12-2014 20:00:00|+18.51|1021.14|62|Scattered clouds
La Plata Buenos Aires Province Argentina|-34.921452,-57.954529|03-12-2014 20:00:00|+22.22|1029.74|93|Clear sky
Lomas de Zamora Buenos Aires Province Argentina|-34.76088,-58.406319|03-12-2014 20:00:00|+22.21|1028.52|84|Clear sky
Lujan Buenos Aires Province Argentina|-32.375702,-65.929604|03-12-2014 20:00:00|+19.36|937.66|63|Broken clouds

7. Project Build: /{Work-Dir}/sbt clean
                 /{Work-Dir}/sbt compile
		 /{Work-Dir}/sbt package

8. Program Execution:

     /{Work-Dir}/spark-submit --class Preprocess ./target/scala-2.10/weather-data-analysis_2.10-1.0.jar 
  
    /{Work-Dir}/spark-submit --class Process ./target/scala-2.10/weather-data-analysis_2.10-1.0.jar 
  
	    
   
 

   
