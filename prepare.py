import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def prepare_store(df):
    """
    Prepares the store DataFrame.
    """
    df['sale_date'] = pd.to_datetime(df['sale_date'],
                                        infer_datetime_format=True)
    df = df.set_index('sale_date').sort_index()
    df['month'] = df.index.month_name()
    df['day'] = df.index.day_name()
    df['sales_total'] = df.sale_amount * df.item_price
    df['Time to ship'] = df['Ship Date'] - df.index
    
    return df

def prepare_germany(df):
    """
    Prepares the Germany power production data set.
    """
    #Changes date/time columns to date-time objects
    df['Date'] = pd.to_datetime(df['Date'])

    #Date-time column to index
    df.set_index('Date', inplace=True).sort_index()

    #Adds month column
    df['Month'] = df.index.month_name()

    #Adds day column    
    df['Day'] = df.index.day_name()

    #Fill null values
    df.fillna(0, inplace=True)
    
    return df

def prepare_superstore(df):
    """
    This function prepares the superstore DataFrame and returns it.
    """
    cols_to_drop = ['Region ID', 'Product ID', 'Category ID']
    
    df.drop(columns=cols_to_drop, inplace=True)
    
    df = df.set_index('Order Date').sort_index()
    
    df['Sales Total'] = (df['Sales'] * df['Quantity']) - df['Discount']
    
    df['Profitable'] = df.Profit > 0
    
    df.Profitable = df.Profitable.astype('int')

    return df