from typing import List
from math import ceil
import datetime

import pandas as pd
import logging
from random import choices

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_batches(
    num_batches: int,
    max_batch_size: int,
    datafile: str,
    current_datetime: datetime.datetime,
) -> tuple[List[List[str]], List[datetime.datetime]]:
    """
    Create a required number of scraping batches. Presumed this will be run once daily

    :param num_batches: Total number of batches to create. Assumed to be a daily value
    :param max_batch_size: Maximum size of each batch. actual size <= max_batch_size
    :param datafile: DataFrame with the product and their update frequency
    :param current_datetime: Date the batches should be run 
    :raises ValueError: if the defined num_batches <= 0 or max_batch_size < 1
    """
    if num_batches <= 0:
        raise ValueError(
            f"Cannot have 0 or negative number of batches. Current number: {num_batches}"
        )
    if max_batch_size < 1:
        raise ValueError(
            f"Cannot have 0 or negative maximum batch size. Current number: {max_batch_size}"
        )

    batches = []
    update_time = []

    # Reading the file with frequency can be done everytime we need to generate a new batch, to get the most updated frequency requirements of each product
    product_frequency = pd.read_csv(datafile)

    total_product_update = product_frequency['Frequency'].sum()
  
    product_frequency['Update_Probability'] = product_frequency['Frequency'] / num_batches

    time_step = datetime.timedelta(minutes=ceil(1440/num_batches))

    average_batch_size = ceil(total_product_update/num_batches)
    batch_size = average_batch_size if average_batch_size < max_batch_size else max_batch_size

    for i in range(num_batches):

        batch = choices(product_frequency['Product'], weights=product_frequency['Update_Probability'], k=batch_size)        
        batches.append(batch)
        update_time.append(current_datetime)

        current_datetime += time_step

    return batches, update_time
