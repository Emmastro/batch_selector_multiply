# Context
At Multiply, we are responsible for changing the prices of products on e-commerce marketplaces such as Amazon. For any particular product, we periodically want to refresh the product information e.g. what are the prices of our competitors, do we have any new reviews. Some products require refreshing more frequently than others depending on their popularity. This is where you come in.

# Batch Jobs Manager
Your team is responsible for refreshing information that we have regarding various products. You have been tasked with creating the batches that will be queued for refresh at any particular time.
You are provided with a table of products with each product having a name and refresh frequency info. Your expected output is a list containing the batches for the day and specifying which products should be included in each batch. For example in the below case:

Product | Frequency
--- | ---
A | 1.0
B | 24.0
C | 0.333

We are aiming for product A to be present in 1 batch per day, B in 24 batches per day and C in 1 batch every 3 days.

The size of any batch may not exceed `max_batch_size`. `batches_per_day` indicates the number of times `create_batch` will be run in a day. For example if `batches_per_day` = 24 and `max_batch_size` = 1000, we can launch upto 24 batching jobs per day, each with the capacity to contain 1000 products for a total refresh capacity of 24000 refreshes in a day. If the sum of required daily product refreshes is 12000, we would expect all batches to contain on average 500 products.

Our 2 main goals are:

1. Averaged over a long time, we would refresh the information of each product with the desired frequency

1. The size of the batches be roughly the same. We would not like some batches to be empty while others are really full

You have been provided with some skeleton code in this repository that should be useful to help you get started. You are free to modify the code (and data) provided as you wish. You may modify any part of any file.

## Further instructions
To do this task, you may, fork, clone, download or otherwise transfer this repository to a place where you are able to edit it. Your final submission should be a publicly available link to a git repository but it doesnt have to be on bitbucket. It could be on Github or Gitlab depending on where you already have an account.