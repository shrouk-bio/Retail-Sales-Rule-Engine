
import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file
import java.nio.file.StandardWatchEventKinds.ENTRY_CREATE
import java.nio.file.{FileSystems, Files, Path, Paths, StandardOpenOption}
import java.sql.{Connection, DriverManager}
import java.time.{LocalDate, LocalDateTime}
import java.time.temporal.ChronoUnit.DAYS
import scala.io.Source.fromFile
import scala.math.BigDecimal.RoundingMode.HALF_UP
import scala.math.{ceil, floor}

// A companion object for the RuleEngine class that contains the main method
object RuleEngine{
  def main(args: Array[String]): Unit = {
    // Create a watch service for monitoring a directory for new files
    val watchService = FileSystems.getDefault.newWatchService()

    //the file need to be modified to the full path of your dir you want to monitor
    // Define the input and output directories
    val input_directory = "E://Scala/in"
    val Output_directory = "E://Scala/out"

    // Register the input directory with the watch service for monitoring
    val path: file.Path = FileSystems.getDefault.getPath(input_directory)
    path.register(watchService, ENTRY_CREATE)

    // Infinite loop for continuously monitoring the input directory for new files
    while (true) {

      // Wait for a new event to occur in the input directory
      val key = watchService.take()

      // Retrieve the events that occurred since the last time we checked
      val events = key.pollEvents()

      // Process each new file in the input directory)
      events.forEach { event =>

        // Get the name and path of the new file
        val fileName = event.context().asInstanceOf[Path].getFileName().toString()
        val filePath = s"$input_directory/$fileName"
        val filePathOutput = s"$Output_directory/$fileName"
        val logPath = "E://Scala/Log/logfile.txt"
        val logFile = Paths.get(logPath)
        if (!Files.exists(logFile)) {
          Files.write(logFile, "\ttimestamp\t\t\tlog_level\t\t\tmessage\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW)
        }
//        if (!Files.exists(logFile)) {
//          Files.write(logFile, "timestamp,log_level,message\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE_NEW)
//        }

        // Read the transactions from the file and apply the qualifying rules to each one
        val transactions = fromFile(filePath).getLines.map(_.split(",").toList).toList.drop(1)
        val transactionsWithDiscounts = transactions.map(applyQualifyingRules)

        // Write the output transactions to a new file in the output directory
        val pw = new PrintWriter(new File(s"$Output_directory/$fileName"))
        pw.write("timestamp,product_name,expiry_date,quantity,unit_price,channel,payment_method,Discount,Final_Price"+"\n")
        transactionsWithDiscounts.foreach(t => pw.write(t.mkString(",") + "\n"))
        pw.close()

        val readSize = transactions.size // here getting the size with header
        val ProcessSize = transactionsWithDiscounts.size //here getting the size
        val receiveMsg = s"${LocalDateTime.now()} \tRECEIVED from file $fileName\t $readSize rows are successfully read from $fileName."
        Files.write(logFile, (receiveMsg + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)

        val processMsg = s"${LocalDateTime.now()} \tPROCESSED to file $fileName\t $ProcessSize rows are successfully written  to output directory and the database."
        Files.write(logFile, (processMsg + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)

//        val receiveMsg = s"${LocalDateTime.now()} ,RECEIVED from file $fileName, $readSize rows are successfully read from $fileName."
//        Files.write(logFile, (receiveMsg + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
//
//        val processMsg = s"${LocalDateTime.now()} ,PROCESSED to file $fileName, $ProcessSize rows are successfully written  to output directory and the database."
//        Files.write(logFile, (processMsg + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)

        // Insert the output transactions into a PostgreSQL database
        val result = insertDatabase(filePathOutput)

        // Delete the input file if it was successfully processed and inserted into the database
        if (result) {
          val fileToDelete = new File(filePath)
          if (fileToDelete.delete()) {
            println("File was Successfully Deleted.")
          } else {
            println("Failed to delete file.")
          }
        } else {
          println(s"Error: Can't insert into database.${fileName}")
        }
      }
      // Reset the watch key to prepare for the next round of monitoring
      key.reset()
    }
  }
  //A method for applying the qualifying rules to a single transaction
  def applyQualifyingRules(transaction: List[String]): List[String] = {

    // Extract the relevant information from the transaction
    val dateOfPurchase = transaction.head // Extract the first element of the list as the date of purchase
    val product = transaction(1) // Extract the second element of the list as the product name
    val expirationDate = transaction(2) // Extract the third element of the list as the expiration date
    val quantity = transaction(3).toInt // Extract the fourth element of the list as the quantity, and convert it to an integer
    val unit_price = transaction(4).toDouble // Extract the fifth element of the list as the unit price, and convert it to a double
    val channel = transaction(5) // Extract the sixth element of the list as the channel
    val payment_method = transaction(6) // Extract the seventh element of the list as the payment method

    // Apply the qualifying rules and calculate the discount and final price
    val discounts = List(
      dateDiscount(dateOfPurchase), // Get the date discount for the purchase date
      productTypeDiscount(product), // Get the product type discount for the purchased product
      expirationDiscount(expirationDate, dateOfPurchase), // Get the expiration date discount for the expiration date and the purchase date
      quantityDiscount(quantity), // Get the quantity discount for the purchased quantity
      paymentDiscount(payment_method), //Get all payment method discount for all payment methods
      channelDiscount(channel,quantity) // Get all channel discounts for all purchased quantity
    ).filter(_ > 0).sorted.reverse // Remove any negative discounts, sort the discounts in descending order

    // Calculate the discount based on the qualifying rules
    val discount = if (discounts.length >= 2) discounts.take(2).sum / 2 // If there are at least two discounts, take the two highest and calculate their average
    else if (discounts.length == 1) discounts.head // If there is only one discount, use it
    else 0.0 // If there are no discounts, the discount is zero
    println(discounts)
    // Calculate the price, final price, and rounded final price
    val price = quantity * unit_price // Calculate the price as the product of the quantity and unit price
    val finalPrice = price * (1 - discount) // Calculate the final price as the price minus the discount
    val roundedPrice = BigDecimal(finalPrice).setScale(2, HALF_UP).toDouble // Round the final price to two decimal places

    // Return the modified transaction as a list of strings, including the discount and rounded price
    transaction ++ List(discount.toString,roundedPrice.toString)
  }


  /**
   * Calculates the discount based on the product channel.
   *
   * @param channel The channel method of the product that is sold.
   * @return The discount to be applied to the product price.
   */
  def channelDiscount(channel: String,quantity:Int): Double = {

    // Define regular expressions to match against the channel method
    val AppRegex = ".*App.*".r

    val discount = channel match {
      case AppRegex() =>
        // the floor and ceil here work the same and thats weired
        //val roundedQuantity = (ceil((quantity - 1) / 5) + 1).toInt * .05
        val roundedQuantity = (floor((quantity - 1) / 5) + 1).toInt * .05
        roundedQuantity
      case _ => 0.0
      // No discount for non-App purchases
    }

    discount
  }

  /**
   * Calculates the discount based on the expiration date of the product.
   *
   * @param expirationDate The expiration date of the product in the format yyyy-MM-dd.
   * @param dateOfPurchase The date of purchase of the product in the format yyyy-MM-dd'T'HH:mm:ss.SSS'Z'.
   * @return The discount to be applied to the product price.
   */

  def expirationDiscount(expirationDate: String,dateOfPurchase:String): Double = {

    // Extract the purchase date from the transaction to be in the format yyyy-MM-dd
    val dataNew=dateOfPurchase.substring(0, 10)

    // Calculate the number of days between the purchase date and the expiration date
    val daysToExpiration = DAYS.between(LocalDate.parse(dataNew), LocalDate.parse(expirationDate))
    // Apply the discount if the product is expiring in less then 30 days
    if (daysToExpiration < 30) {
      (30 - daysToExpiration) *0.01
    } else {
      0
    }
  }

  /**
   * Calculates the discount based on the product Type.
   *
   * @param product The product type .
   * @return The discount to be applied to the product price.
   */

  def productTypeDiscount(product: String): Double = {
    // Define regular expressions to match against the product name
    val wineRegex = ".*Wine.*".r
    val cheeseRegex=".*Cheese.*".r

    // Use pattern matching to determine the discount based on the product name
    product match {
      case wineRegex() => 0.05 // 5% discount for wine products
      case cheeseRegex() => 0.10 // 10% discount for cheese products
      case _ => 0 // no discount for other products
    }
  }

  /**
   * Calculates the discount based on the product purchase date .
   *
   * @param dateOfPurchase The date of purchase of the product in the format yyyy-MM-dd'T'HH:mm:ss.SSS'Z'.
   * @return The discount to be applied to the product price.
   */
  def dateDiscount(dateOfPurchase: String): Double = {

    // Extract the purchase date from the transaction to be in the format yyyy-MM-dd
    val modifydata = dateOfPurchase.substring(0, 10)
    // Check if the date matches a qualifying date for a discount
    modifydata match {
      case "2023-03-23" => 0.5 // 50% discount for this date
      case _ => 0 //else eny other date will have discount 0
    }
  }

  /**
   * Calculates the discount based on the product Quantity.
   *
   * @param quantity The quantity of the product that is sold.
   * @return The discount to be applied to the product price.
   */
  def quantityDiscount(quantity: Int): Double = {

    // Check the quantity of products purchased and return the corresponding discount
    quantity match {
      case q if q >= 6 && q <= 9 => 0.05 // Apply 5% discount if quantity is between 6 and 9 (inclusive)
      case q if q >= 10 && q <= 14 => 0.07 // Apply 7% discount if quantity is between 10 and 14 (inclusive)
      case q if q >= 15 => 0.10 // Apply 10% discount if quantity is 15 or greater
      case _ => 0 // else no discounts will be applied for other quantities
    }
  }

  /**
   * Calculates the discount based on the product payment method.
   *
   * @param payment_method The payment method of the product that is sold.
   * @return The discount to be applied to the product price.
   */
  def paymentDiscount(payment_method: String): Double = {

    // Define regular expressions to match against the payment method
    val VisaRegex = ".*Visa.*".r

    // Check the quantity of products purchased and return the corresponding discount
    payment_method match {
      case VisaRegex() => 0.05 // 5% discount for Visa method.
      case _ => 0 // else no discounts for cash method.
    }
  }
  /**
   * Inserts transaction data from a fisle into a PostgreSQL database.
   * @param filePath the path to the file containing transaction data
   * @return true if the data was successfully inserted, false otherwise
   */

  def insertDatabase(filePath: String): Boolean = {

    // Establish a connection to the database
    val connection: Connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "mmma")

    // Define the SQL query to insert transaction data into the database
    val insertQuery = "INSERT INTO transactions (timestamp, product_name, expiry_date, quantity, unit_price, channel, payment_method, discount, final_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

    // Prepare the SQL statement
    val preparedStatement = connection.prepareStatement(insertQuery)
    try {
      // Read transaction data from the file and execute the SQL statement for each transaction
      val transactions = io.Source.fromFile(filePath).getLines.map(_.split(",").toList).toList.drop(1)
      transactions.foreach { transaction =>
        preparedStatement.setString(1, transaction.head)
        preparedStatement.setString(2, transaction(1))
        preparedStatement.setString(3, transaction(2))
        preparedStatement.setInt(4, transaction(3).toInt)
        preparedStatement.setDouble(5, transaction(4).toDouble)
        preparedStatement.setString(6, transaction(5))
        preparedStatement.setString(7, transaction(6))
        preparedStatement.setDouble(8, transaction(7).toDouble)
        preparedStatement.setDouble(9, transaction(8).toDouble)
        preparedStatement.executeUpdate()
      }
      // Return true if all transactions were successfully inserted to the database
      true
    } catch {
      // If an exception occurs during execution, catch it and return false
      case e: Exception =>
        false
    }
    // Close the statement and connection, if they were successfully opened
    //finally {
    //preparedStatement.close()
    //connection.close()
    //    }
  }
}
