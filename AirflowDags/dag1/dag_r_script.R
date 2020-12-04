library(RMariaDB)
library(ggplot2)
library(jsonlite)

password = fromJSON('/home/tom/sql_password_json.json') 
db <- dbConnect(RMariaDB::MariaDB(), user = 'root', password = password$password,
                dbname = 'dag_data') #connecting to MySQL
SQL <- "SELECT * FROM bookstore_data"
query <- dbSendQuery(db, SQL) #sending query to db
df <- dbFetch(query) #fetching result from query

fig <- ggplot(df, aes(x = title, y = price)) + 
  geom_bar(stat = 'identity', fill = 'green', colour = 'black') +
  xlab("title") + ylab("Price (£)") + ggtitle("Book Prices")

ggsave('/home/tom/Documents/r_files/bookstore_graph.png', plot = fig)