import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import RobustScaler, MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error


#Removes warnings and imporves asthenics
import warnings
warnings.filterwarnings("ignore")

from env import get_connection

def star_wars_people():
    
    """ 
    This function gets data for Star Wars:
    people
    planets
    starships
    """

    filename1 = "people_starwars.csv"
    
    if os.path.isfile(filename1):
        return pd.read_csv(filename1)
    else:
    
        response = requests.get('https://swapi.dev/api/people/')
        data = response.json()
        people = pd.DataFrame(data['results'])
        while next_page != None:
            response = requests.get(data['next'])
            data = response.json()

            number_of_people = data['count']
            next_page = data['next']
            previous_page = data['previous']
            number_of_results = len(data['results'])
            max_page = math.ceil(number_of_people / number_of_results)

            people = pd.concat([people, pd.DataFrame(data['results'])],
                               ignore_index=True)
        
        people.to_csv(filename1, index=False)
        return people
    

def star_wars_planets():

        
    filename2 = "planets_starwars.csv"

    if os.path.isfile(filename2):
        return pd.read_csv(filename2)
    else:
        
        url = 'https://swapi.dev/api/planets'
        response = requests.get(url)    
        planets = pd.DataFrame(data['results'])
        while next_page != None:
            response = requests.get(data['next'])
            data = response.json()

            next_page = data['next']

            planets = pd.concat([planets,
                                 pd.DataFrame(data['results'])],
                                ignore_index=True)
        
        planets.to_csv(filename2, index=False)
        return planets

def star_wars_starships():

    filename3 = "starships_starwars.csv"

    if os.path.isfile(filename3):
        return pd.read_csv(filename3)
    else:
    
        url = 'https://swapi.dev/api/starships'
        response = requests.get(url)
        data = response.json()
        starships = pd.DataFrame(data['results'])

        while data['next'] != None:
            response = requests.get(data['next'])
            data = response.json()

            #next_page = data['next']

            starships = pd.concat([starships,
                                   pd.DataFrame(data['results'])],
                                  ignore_index=True)

        starships.to_csv(filename3, index=False)
    
        return starships

def wrangle_star_wars(return_ugly=False):
    """
    Returns the star wars data.
    """
    people = star_wars_people()
    planets = star_wars_planets()
    starships = star_wars_starships()
    
    #Not working. Returns a tuple for some reason.
    
    if return_ugly == "True":
        ugly_df = pd.DataFrame()
        ugly_df = pd.concat([people, starships])
        ugly_df = pd.concat([ugly_df, planets])
        return ugly_df
    
    else:
    
        return people, planets, starships


def wrangle_germany_power():
    """Gets data for the German power system"""
    
    filename = "germany_power.csv"
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
        data = pd.read_csv(url)
        data.to_csv(filename, index=False)
        return data

def wrangle_zillow():
    """
    This function reads the zillow data from Codeup db into a df.
    Changes the names to be more readable.
    Drops null values.
    """
    filename = "full_zillow_2017.csv"
    
    if os.path.isfile(filename):
        return pd.read_csv(filename, parse_dates=['transactiondate'])
    else:
        
        # read the SQL query into a dataframe
        query = """
        SELECT * FROM properties_2017
        LEFT JOIN airconditioningtype USING (airconditioningtypeid)
        LEFT JOIN architecturalstyletype USING (architecturalstyletypeid)
        LEFT JOIN buildingclasstype USING (buildingclasstypeid)
        LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
        LEFT JOIN predictions_2017 USING (parcelid)
        LEFT JOIN propertylandusetype USING (propertylandusetypeid)
        LEFT JOIN storytype USING (storytypeid)
        LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
        LEFT JOIN unique_properties USING (parcelid)
        WHERE transactiondate LIKE "2017%%";
        """

        df = pd.read_sql(query, get_connection('zillow'))
        df['latitude'] = df['latitude'] / 10_000_000
        df['longitude'] = df['longitude'] / 10_000_000
        df.drop(columns-'id', inplace=True)
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)
        
        # Return the dataframe to the calling code
        return df
    
def wrangle_mall():
    """
    This function gets all data from the mall_customers database.
    """
    filename = "mall_customers.csv"
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        
        # read the SQL query into a dataframe
        query = """
        SELECT * FROM customers;
        """

        df = pd.read_sql(query, get_connection('mall_customers'))
        
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)
        
        # Return the dataframe to the calling code
        return df
    
def tsa_item_demand():
    """
    This function gets all data from the mall_customers database.
    """
    filename = "tsa_item_demand.csv"
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        
        # read the SQL query into a dataframe
        query = """
        SELECT * FROM sales JOIN items USING (item_id) JOIN stores USING (store_id);
        """

        df = pd.read_sql(query, get_connection('tsa_item_demand'))
        
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)
        
        # Return the dataframe to the calling code
        return df

def acquire_store():
    
    filename = 'store.csv'
    
    if os.path.exists(filename):
        
        return pd.read_csv(filename)
    
    else:
        
        query = '''
                SELECT sale_date, sale_amount,
                item_brand, item_name, item_price,
                store_address, store_zipcode
                FROM sales
                LEFT JOIN items USING(item_id)
                LEFT JOIN stores USING(store_id)
                '''
        
        url = get_connection(db='tsa_item_demand')
        
        df = pd.read_sql(query, url)
        
        df.to_csv(filename, index=False)
        
        return df
    

def wrangle_iris():
    """
    This function gets all data from the iris database.
    """
    filename = "iris_db.csv"
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    else:
        
        # read the SQL query into a dataframe
        query = """
        SELECT * FROM measurements 
        LEFT JOIN species USING (species_id);
        """

        df = pd.read_sql(query, get_connection('iris_db'))
        
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)
        
        # Return the dataframe to the calling code
        return df
    
def get_superstore_data():

    """
    This function gets all data from the superstore database.
    """
    filename = "superstore.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename,  parse_dates=['Order Date', 'Ship Date'])
    else:

        # read the SQL query into a dataframe
        query = """ 
        SELECT * FROM orders
        LEFT JOIN categories USING (`Category ID`)
        LEFT JOIN customers USING (`Customer ID`)
        LEFT JOIN products USING (`Product ID`)
        LEFT JOIN regions USING (`Region ID`);
        """

        df = pd.read_sql(query, get_connection('superstore_db'))

        df['Order Date'] = pd.to_datetime(df['Order Date'],
                                          infer_datetime_format=True)
        df['Ship Date'] = pd.to_datetime(df['Ship Date'],
                                         infer_datetime_format=True)        
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)

        # Return the dataframe to the calling code
        return df