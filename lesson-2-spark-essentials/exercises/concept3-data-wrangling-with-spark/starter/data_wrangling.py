# Take care of any imports

# Create the Spark Context

# Complete the script


path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"


# # Data Exploration 
# 
# # Explore the data set.


# View 5 records 

# Print the schema


# Describe the dataframe


# Describe the statistics for the song length column


# Count the rows in the dataframe
   

# Select the page column, drop the duplicates, and sort by page


# Select data for all pages where userId is 1046


# # Calculate Statistics by Hour


# Select just the NextSong page


# # Drop Rows with Missing Values


# How many are there now that we dropped rows with null userId or sessionId?
)

# select all unique user ids into a dataframe


# Select only data for where the userId column isn't an empty string (different from null)


# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 



# Create a user defined function to return a 1 if the record contains a downgrade


# Select data including the user defined function



# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.



# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have



# Show the phases for user 1138 

