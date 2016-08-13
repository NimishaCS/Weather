/*** Weather Data Analysis ****/

// Gopinathan K.M Dated: 12/08/2016

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.io.Source
import java.io._

import java.util.Date
import java.text.SimpleDateFormat
import java.text.DecimalFormat

object Process {

val sc 	= new SparkContext(new SparkConf().setAppName("Weather Data Analysis"))

//--------------------------------------------------------------------------------------------------------------------//
// Function to convert Unix timestamp to ISO8601 date time

def convertDate(dt: Int) : String = {
      val st = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(new Date(dt * 1000L))
      return st
      }

//---------------------------------------------------------------------------------------------------------------------//
// Main method
    def main(args: Array[String]) {
    	
// Catch ALL --- invlaid records where record itself is BLANK

val x0 = sc.textFile("hdfs://quickstart.cloudera:8020/weather/preprocessed.txt").filter(_.length > 1)

// Removing ALL headers from RDD.
val x1 = x0.filter(!_.contains("Weather_ID")).cache

//---------------------------------------------------------------------------------------------------------------------//
// Function convert to 2 decimal positions and formatting
def roundTo2Decimals(temp: Double) : String = { val df2 = new DecimalFormat("##.##") 
 val st = (df2.format(temp).toDouble)
   if (st > 1.00) {
   return ( "+" + (st.toString))
   }
   else { 
   return (st.toString)
   }
   
}

//-------------------------------------------------------------------------------------------------------------------------//
// Function to get weather description

def getDesc(id: Int) : String = id match {

case 200  =>	"Thunderstorm with light rain"	 
case 201  =>	"Thunderstorm with rain"	 
case 202  =>	"Thunderstorm with heavy rain"	  
case 210  =>	"Light Thunderstorm"	  
case 211  =>	"Thunderstorm"	  
case 212  =>	"Heavy thunderstorm"	  
case 221  =>	"Ragged thunderstorm"	  
case 230  =>	"Thunderstorm with light drizzle"	  
case 231  =>	"Thunderstorm with drizzle"	  
case 232  =>	"Thunderstorm with heavy drizzle"	  
case 300  =>	"Light intensity drizzle"	 
case 301  =>	"Drizzle"	  
case 302  =>	"Heavy intensity drizzle"	  
case 310  =>	"Light intensity drizzle rain"	  
case 311  =>	"Drizzle rain"	  
case 312  =>	"Heavy intensity drizzle rain"	  
case 313  =>	"Shower rain and drizzle"	  
case 314  =>	"Heavy shower rain and drizzle"	  
case 321  =>	"Shower drizzle"	  
case 500  =>	"Light rain"	  
case 501  =>	"Moderate rain"	  
case 502  =>	"Heavy intensity rain"	 
case 503  =>	"Very heavy rain"	  
case 504  =>	"Extreme rain"	  
case 511  =>	"Freezing rain"	 
case 520  =>	"Light intensity shower rain"	  
case 521  =>	"Shower rain"	  
case 522  =>	"Heavy intensity shower rain"	  
case 531  =>	"Ragged shower rain"	  
case 600  =>	"Light snow"	
case 601  =>	"Snow"	 
case 602  =>	"Heavy snow"	 
case 611  =>	"Sleet"	 
case 612  =>	"Shower sleet"	 
case 615  =>	"Light rain and snow"	 
case 616  =>	"Rain and snow"	 
case 620  =>	"Light shower snow"	 
case 621  =>	"Shower snow"	 
case 622  =>	"Heavy shower snow"	 
case 701  =>	"Mist"	
case 711  =>	"Smoke"	 
case 721  =>	"Haze"
case 731  =>	"Sand, dust whirls"	 
case 741  =>	"Fog"	 
case 751  =>	"Sand"	 
case 761  =>	"Dust"	 
case 762  =>	"Volcanic ash"	 
case 771  =>	"Squalls"	 
case 781  =>	"Tornado"	 
case 800  =>	"Clear sky"	
case 801  =>	"Few clouds"	
case 802  =>	"Scattered clouds"	 
case 803  =>	"Broken clouds"	 
case 804  =>	"Overcast clouds"	
case 900  =>	"Tornado"
case 901  =>	"Tropical storm"
case 902  =>	"Hurricane"
case 903  =>	"Cold"
case 904  =>	"Hot"
case 905  =>	"Windy"
case 906  =>	"Hail"
case 951  =>	"Calm"
case 952  =>	"Light breeze"
case 953  =>	"Gentle breeze"
case 954  =>	"Moderate breeze"
case 955  =>	"Fresh breeze"
case 956  =>	"Strong breeze"
case 957  =>	"High wind, near gale"
case 958  =>	"Gale"
case 959  =>	"Severe gale"
case 960  =>	"Storm"
case 961  =>	"Violent storm"
case 962  =>	"Hurricane"

}
//-------------------------------------------------------------------------------------------------------------------------//

//Mapping RDD and Applying functions, convertDate() and converting temperature Kelvin celsious to Degree celsious

val x2 = x1.map(line  => line.split('\t')).map(line => (line(0).replaceAll("[^\\p{L}\\p{Nd}]+", " ").trim, line(1).replaceAll("\"" , "").trim, convertDate(line(2).trim.toInt),  roundTo2Decimals((line(3).trim.toFloat- 273.15)), line(4).trim.toFloat, line(5).trim.toInt, getDesc(line(6).trim.toInt) ))



//  Saving to Text File after applying "|" delimeter

val path = "./data/saveFile.txt"

val x3 = x2.map(f => f._1+"|"+f._2+"|"+f._3+"|"+f._4+"|"+f._5+"|"+f._6+"|"+f._7).saveAsTextFile(path)

	}

//------------------------------------------------main function ends------------------------------------------------------// 

} 
//------------------------------------------------class -----------------------------------------------------------------//



