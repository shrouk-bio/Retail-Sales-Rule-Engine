# Retail-Sales-Rule-Engine
Retail Stores Sales Rule-Engine With Scala Functional Programming
This is a Scala program that applies qualifying rules to transactions, writes the output transactions to a file, and inserts them into a PostgreSQL database. The program also monitors an input directory for new files, processes the files, and deletes the input files if they were successfully processed and inserted into the database.

Installation
Install Scala
Install sbt
Install PostgreSQL
Clone this repository using git clone https://github.com/yourusername/Scala-Rule-Engine.git
# Usage
Navigate to the cloned repository: cd Scala-Rule-Engine
Compile the program: sbt compile
Run the program: sbt run
Place input files in the in directory
Output files will be written to the out directory
Logs will be written to the Log directory

# Configuration
You can configure the input and output directories in the RuleEngine.scala file. 
You can also configure the PostgreSQL database connection in the DatabaseConnection.scala file.

# Contributing
Contributions are welcome! Please fork this repository and submit a pull request with your changes.


# The Project Simulates a discount calculator for a sales system that offers discounts based on various rules and conditions. The rules are as follows:


# Qualifying Rules
Products with less than 30 days remaining until expiration (from the day of transaction) are eligible for discounts.
Cheese and wine products are currently on sale.
Sales made using Visa cards are eligible for a minor discount.

# Calculation Rules
Discounts are calculated based on the number of days remaining until the product's expiration date:
If 29 days remaining -> 1% discount
If 28 days remaining -> 2% discount
If 27 days remaining -> 3% discount
And so on...
Cheese products are eligible for a 10% discount.
Wine products are eligible for a 5% discount.
Products sold on 23rd March have a special discount of 50%.
If more than 5 of the same product are bought:
6-9 units -> 5% discount
10-14 units -> 7% discount
More than 15 units -> 10% discount
Sales made through the app are eligible for a special discount, based on the quantity rounded up to the nearest multiple of 5:
Quantity: 1, 2, 3, 4, 5 -> Discount: 5%
Quantity: 6, 7, 8, 9, 10 -> Discount: 10%
Quantity: 11, 12, 13, 14, 15 -> Discount: 15%
And so on...

# How to Use
To use this discount calculator, simply input the product's expiration date, type of product, quantity, and payment method, and the calculator will output the discounted price.
