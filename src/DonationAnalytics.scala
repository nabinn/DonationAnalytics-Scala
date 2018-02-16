import scala.io.Source
import java.io.{FileNotFoundException, IOException, PrintWriter, File}
import scala.collection.mutable.{Map,HashMap}
import scala.collection.immutable.{Map => immutableMap}
import java.time.format.{DateTimeFormatter, DateTimeParseException} 
import java.time.LocalDate
import util.control.Breaks._
import scala.util.Sorting

object DonationAnalytics {
  
  def getFieldToIndex():immutableMap[String, Int]= {
    //data fields as given here: https://goo.gl/89YTjW
    val FIELDS:String = """ CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,
        TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,
        OCCUPATION,TRANSACTION_DT,TRANSACTION_AMT,OTHER_ID,TRAN_ID,
        FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID"""
    
    val FIELD_LIST = FIELDS.replaceAll(" ", "").replaceAll("\n", "").replaceAll("\t", "").split(",")
   /*
    val field2Index:Map[String, Int] = new HashMap()
    for ( idx <- 0 until FIELD_LIST.length ){
      field2Index += (FIELD_LIST(idx) -> idx)
    }    
    
    field2Index
    */
    (FIELD_LIST zip (0 until FIELD_LIST.length)).toMap
  }
  
  
  def getPercentileIndex(n:Int, p:Int):Int={
    val ordinalRank = Math.ceil(p * n *1.0/100).toInt
    return ordinalRank - 1
  }
  
  
  def isValidDate(dateStr:String):Boolean = try{
        val formatter = DateTimeFormatter.ofPattern("MMddyyyy")
        val date = LocalDate.parse(dateStr, formatter)
        true
    }
    catch {
      case e: DateTimeParseException => false
    }
    
  
  def getYear(dateStr:String):Int= {
    if(isValidDate(dateStr))
      dateStr.takeRight(4).toInt
    else
      -1
  }
  
  
  def readPercentileFromFile(filename: String): Int = {
    try {
       val content = Source.fromFile(filename).getLines.mkString
       return content.toInt
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.");return -1                                       
      case e: IOException => println("Got an IOException!"); return -1
      case _: Throwable => println("Not a valid percentile value"); return -1
    }
  }
  
  
  def readFileAndProcessData(inputDataFile:String, outputFile:String, percentileValue:Int):Unit={
    
    val donors:Map[(String, String),Int] = new HashMap()
    val donation_count:Map[(String, String, Int),Int] = new HashMap()
    val donation_sum:Map[(String, String, Int),Int] = new HashMap()
    val donations:Map[(String, String, Int),Array[Int]] = new HashMap()
    
    
    val field2Index:immutableMap[String, Int]=getFieldToIndex
    
    val source = Source.fromFile(inputDataFile)
    val destination = new PrintWriter(new File(outputFile))
    
    for (line <- source.getLines()){
        val record = line.split('|')
        breakable{ //breakable level 1
          if(record.length != field2Index.size){
             break
          }else{
             val cmteId = record(field2Index("CMTE_ID"))
             val otherId = record (field2Index("OTHER_ID"))  
             val name = record(field2Index("NAME"))
             val zipCode = record(field2Index("ZIP_CODE"))
             val transactionDt = record(field2Index("TRANSACTION_DT"))
             val transactionAmt = record(field2Index("TRANSACTION_AMT"))
             
             breakable{ //breakable level 2
               if ((cmteId.length == 0) || (otherId.length != 0) || 
                   (name.length == 0) ||(zipCode.length < 5) || 
                   (transactionAmt.length == 0) || (!isValidDate(transactionDt))) {
               break
               }else{
                 val amount = Math.ceil(transactionAmt.toDouble).toInt
                 
                 breakable{//breakable level 3
                   if (amount < 0){
                     break
                   }else{
                     val zip = zipCode.take(5)
                     val year = getYear(transactionDt)
                     //this block has valid records

                     if (donations.contains((cmteId, zip, year))){
                       donation_count(cmteId, zip, year) += 1
                       donation_sum(cmteId, zip, year) += amount
                       donations(cmteId, zip, year) ++ Array(amount)
                       
                     }else{
                       donation_count += (cmteId, zip, year) -> 1
                       donation_sum += (cmteId, zip, year) -> amount
                       donations += (cmteId, zip, year) -> Array(amount)
                     }
                     
                    // if this donor is a repeat donor, write data to output file
                    if (donors.contains(name, zip) && year > donors(name, zip)){
                        val numDonations = donation_count(cmteId, zip, year)
                        val totalAmt = donation_sum(cmteId, zip, year)
                        val percentileIdx = getPercentileIndex(numDonations, percentileValue)
                        //Sorting.quickSort(donations(cmteId, zip, year))
                        val sortedDonations = donations(cmteId, zip, year).sorted
                        val percentileAmt = sortedDonations(percentileIdx)
                        destination.write(s"$cmteId|$zip|$year|$percentileAmt|$totalAmt|$numDonations\n")
                    }else{
                        // add this donor to donors
                        donors += (name, zip) -> year
                    }                   
                   }
                 }//end of breakable level 3
               }
             }//end of breakable level 2   
          }
      }//end of breakable level 1
        
    }// end of for loop
    source.close //close the file
    destination.close
    //for ((k,v) <- donations) printf("key: %s, value: %s\n", k, v)
    //for(item <- donations("C00384818","02895",2017)) println(item)
  }
  
  
  def main(args:Array[String]):Unit={
    
    if (args.length < 3){
      println("Usage:")
      println("scala ./src/DonationAnalytics.scala ./input/itcont.txt ./input/percentile.txt ./output/repeat_donors.txt")
      System.exit(1)
    }else{
     
      val start:Long = System.currentTimeMillis();
      val inputDataFile:String = args(0) // main data file
      val inputPercentileFile:String = args(1) // text file containing percentile value
      val outputFile:String = args(2) //output file to store result
      
      val percentile_value:Int = readPercentileFromFile(inputPercentileFile)
      
      if (percentile_value < 0 || percentile_value > 100){
        println("percentile must be between 0 and 100")
        System.exit(1)
      }
      //println(s"percentile = $percentile_value")
      readFileAndProcessData(inputDataFile, outputFile, percentile_value)
      
      val end:Long = System.currentTimeMillis();
      println(s"total elapsed time = ${(end-start)*1.0/1000} s")
    }
    
  }
  
}
