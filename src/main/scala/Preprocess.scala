/*** Weather Data Analysis ****/

// Gopinathan K.M  Dated: 12/08/2016

// Pre-processing initial input file by removing repeating records.


//import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf

import scala.io.Source
import java.io._

object Preprocess {

//val sc = new SparkContext(new SparkConf().setAppName("Weather Data Analysis"))


//------------------------------------------------------------------------------------------//
// Main method
    def main(args: Array[String]) {
    	val inputFile 	= "./data/weather_details.txt"
        preprocessing(inputFile) 
	
    }
//------------------------------------------------main function ends-------------------------// 

//------------------------------------------------preprocessing function starts-----------------// 
 def preprocessing(file: String) {
	var previousCity = " "
	var currentCity = " "
	var line = " "
	val outputFile = "./data/preprocessed.txt"
	val writer = new PrintWriter(new File(outputFile))

// Loop through each line in the file

        for (line <- Source.fromFile(file).getLines()) {
         
	val arr = line.split("\t").toArray
                currentCity = arr(0)
                if (currentCity != previousCity) {
                        	//println(line)
		writer.println(line)
		}
          		previousCity = currentCity
        	             }
              	 writer.close()
	
	} 
//------------------------------------------------preprocessing function ends-------------------//
    

} 
 //------------------------------------------------class ends-----------------------------------//

