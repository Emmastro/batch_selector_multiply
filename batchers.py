from typing import List
from math import ceil
import datetime

import pandas as pd
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def create_batches(
    num_batches: int,
    max_batch_size: int,
    product_frequency: pd.DataFrame,
    current_datetime: datetime.datetime,
    n_days: int,
) -> tuple[List[List[str]], List[datetime.datetime]]:
    """
    Create a required number of scraping batches. Presumed this will be run once daily

    :param num_batches: Total number of batches to create. Assumed to be a daily value
    :param max_batch_size: Maximum size of each batch. actual size <= max_batch_size
    :param product_frequency: DataFrame with the product and their update frequency
    :param current_datetime: Date the batches should be run 
    :param n_days: days passed since the first run of batches
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
    product_frequency['Frequency_adjusted'] = product_frequency['Frequency'].apply(
        lambda frequency: adjust_frequency(frequency, n_days))

    total_product_update = product_frequency['Frequency_adjusted'].sum()
    max_update = product_frequency['Frequency_adjusted'].max()

    # Sort product by highest frequency in descending order so as to have all product that require the most update
    # at the top, and optimize for empty batches at the end 
    product_frequency = product_frequency.sort_values(
        by="Frequency_adjusted", ascending=False)

    if max_update > num_batches:
        # TODO: review warning text, and set more context
        logging.warning(
            "There is a product that requires more updates than the set number of batches on a day")

    # You will need to fake moving time forward. You are free to decide
    # How small/big each time step will be

    # Move time_step based on the product with the largest frequency.
    time_step = datetime.timedelta(minutes=ceil(1440/num_batches))

    average_batch_size = ceil(total_product_update/num_batches)

    batch_size = average_batch_size if average_batch_size < max_batch_size else max_batch_size

    for i in range(num_batches):

        batch = create_batch(batch_size, product_frequency)
        
        # We can check if the batch will be empty before running the batch, but that will 
        # take more computation than running the batch and checking if it is empty
        if len(batch) == 0:
            logging.info(
                f"""No more batch to execute from the product_frequency file. 
                Number of batches set to {num_batches} but only required {i} batches. \n
                Day: {current_datetime}, ({n_days}) days from start""")
            break

        batches.append(batch)
        update_time.append(current_datetime)

        current_datetime += time_step

    return batches, update_time


def adjust_frequency(frequency, n_days):
    """
    Adjust frequency for products with frequency < 1.

    :param frequency: Update frequency of the product
    """
    if frequency < 1:
        if n_days % int(1/frequency) == 0:
            return 1
        else:
            return 0

    return frequency


def create_batch(
        batch_size: int,
        product_frequency: pd.DataFrame) -> List[str]:
    """ 
    :param batch_size: Maximum size of each batch. 
    :param product_frequency: DataFrame with the product and their update frequency
    """

    batch = []
    for index, row in product_frequency.iterrows():

        if len(batch) >= batch_size:
            break

        if row['Frequency_adjusted'] > 0:
            product_frequency.loc[index, 'Frequency_adjusted'] -= 1
            batch.append(row['Product'])

    return batch
