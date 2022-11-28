# %%
"""
Multiply developer skill assessment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This file and other files within this repository are used for a developer skill
assessment by Multiply (multiply.cloud).

If you have been asked to attempt this assessment, you are free to modify 
any of the files or even create new files if necessary in order to get the 
outcome required in this task.
"""

import datetime
import os
import pandas as pd

from batchers import create_batches

# %%

datafile = "db_large.csv"
# datafile = "db.csv"

num_days = 7

# for small db
# DAILY_BATCHES = 6
# MAX_BATCH_SIZE = 2

# for large db
DAILY_BATCHES = 40
MAX_BATCH_SIZE = 6

# %%
curr_datetime = datetime.datetime.now()
# Create batches for the next num_days days
for i in range(num_days):

    batches, update_time = create_batches(
        DAILY_BATCHES,
        MAX_BATCH_SIZE,
        datafile,
        curr_datetime)  # Add your create_batches args here
    curr_datetime += datetime.timedelta(days=1)

    # check if folder exist, create if it doesn't
    if not os.path.exists("batches"):
        os.mkdir("batches")

    batches = pd.DataFrame(
        {"batch": batches, "update_time": update_time})
    batches.to_csv(f"batches/batch_{i}.csv", index=False)

