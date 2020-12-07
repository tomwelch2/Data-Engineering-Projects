library(dplyr)
library(tidyr)
library(ggplot2)
library(aws.s3)
library(jsonlite)
library(RMariaDB)

AWS_CREDS <- fromJSON('/home/tom/Documents/AWS_CREDS.json') #reading in creds from JSON file

Sys.setenv("AWS_ACCESS_KEY_ID" = AWS_CREDS$aws_access_key_id,
           "AWS_SECRET_ACCESS_KEY" = AWS_CREDS$aws_secret_access_key_id,
           "AWS_DEFAULT_REGION" = "eu-west-2") #setting S3 credentials to connect to S3

save_object("sales_records.csv", bucket = "testingbucket1003", file = "/home/tom/Documents/csv_files/r_aws_data.csv")

df <- read.csv('/home/tom/Documents/csv_files/r_aws_data.csv') #reading in CSV file from S3
ship_dates <- as.Date(df$Ship.Date, "%d/%m/%Y") #converting dates to date-type
order_dates <- as.Date(df$Order.Date, "%d/%m/%Y")
df <- df %>% mutate(ship_date = ship_dates)
df <- df %>% mutate(order_date = order_dates)

df <- df %>% drop_na() #dropping nulls
df <- df %>% select(-c(Ship.Date, Order.Date)) 

groups <- df %>% group_by(Region)
aggregate_data <- groups %>% summarize(mean_profit = mean(Total.Profit))
aggregate_data <- as.data.frame(aggregate_data)
aggregate_data
fig <- ggplot(aggregate_data, aes(x = Region, y = mean_profit)) + geom_bar(stat = 'identity', fill = "orange", colour = 'black') +
  ggtitle("Average Profit By Continent") + xlab("Continent") + ylab("Profit ($)")

ggsave('/home/tom/Documents/r_files/r_aws_pipeline_graph.png', plot = fig)

mysql_password <- fromJSON("/home/tom/mysql_creds.json")
db <- dbConnect(RMariaDB::MariaDB(), user = 'root', host = 'localhost', password = mysql_password$password,
                dbname = "aws_data") #connecting to RDBMS

dbWriteTable(db, "aws_r_pipeline", df) #sends data to MySQL database




