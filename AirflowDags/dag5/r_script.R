library(ggplot2)
library(dplyr)
library(tidyr)

df <- read.csv("/home/tom/Documents/csv_files/aws_sales_csv.csv")
groups <- df %>% group_by(Region)
aggregate = groups %>% summarize(mean_profit = mean(Total.Profit))

fig <- ggplot(aggregate, aes(x = Region, y = mean_profit)) + 
  geom_bar(stat = 'identity', fill = "red", colour = "black") +
  ggtitle("Average Profit By Continent") + xlab("Continent") +
  ylab("Average Total Profit ($)")

ggsave('/home/tom/Documents/csv_files/aws_sales_r_graph.png',
       plot = fig)