'''
World Reporter
==============

Extract all of the World Reporter data (specifically abstracts). 
We use a seed list of URLs (one for each abstract) by
selecting all years and exporting a CSV from
https://worldreport.nih.gov/app/#!/. Note that this process 
is super slow and so that step was performed in browser
manually.

For colleagues at Nesta: the CSV can be accessed in the AWS
bucket `nesta-inputs` at key `world_reporter_inputs.csv`.
'''

from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
import boto3
import pandas as pd
from io import BytesIO
from retrying import retry
import re
import requests
import time
import traceback


s3 = boto3.resource('s3')
# Path to the abstract element
ELEMENT_EXISTS = EC.visibility_of_element_located((By.CSS_SELECTOR,
                                                   ("#dataViewTable > tbody > "
                                                    "tr.expanded-view.ng-scope > "
                                                    "td > div > div > p")))

#@retry(wait_random_min=20, wait_random_max=150, stop_max_attempt_number=10)
def get_abstract(url):
    '''Wait for the abstract to appear at the URL

    Args:
        url (str): An abstract URL specified from the input CSV file.
    '''
    text = None
    ntries = 0
    while text is None:
        browser = None
        ntries += 1
        assert ntries < 10, "Max retries exceeded"
        # Fire up a browser
        try:
            options = Options()
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-extensions")
            browser = webdriver.Chrome(chrome_options=options)
            browser.get(url)
            time.sleep(10)
            # Wait until the element appears
            wait = WebDriverWait(browser, 60)
            element = wait.until(ELEMENT_EXISTS)
        except (TimeoutException, WebDriverException, ConnectionResetError):
            print("Retrying", url)
            print(traceback.format_exc())
            time.sleep(60)
        else:
            text = element.text
        finally:
            # Tidy up and exit
            if browser is not None:
                browser.quit()

    return text.encode("utf-8")


def get_csv_data():
    '''Retrieve the CSV data from AWS and tidy the columns'''
    obj = s3.Object("nesta-inputs", "world_reporter_inputs.csv")
    bio = BytesIO(obj.get()['Body'].read())
    df = pd.read_csv(bio)
    df.columns = [col.replace(" ","_").lower() for col in df.columns]
    # Create a field ready for the abstract text
    df["abstract_text"] = None
    return df


def extract_year(date):
    '''
    Use a regex search for 4 digits in a row to identify the year.

    Args:
        date (str): The full date string.
    '''
    try:
        return re.search(r'\d{4}', date).group(0)
    except (TypeError, AttributeError):
        return None


def geocode(city, country):
    '''
    Geocoder using openstreetmap API.

    Requests must be limited to 1 per second and cached locally.

    Args:
        city (str): name of the city.
        country (str): name of the country.
    '''
    headers = {'User-Agent': 'City Geocoder'}
    url = f'https://nominatim.openstreetmap.org/search?city={city}&country={country}&format=json'
    try:
        response = requests.get(url, headers=headers)
    except requests.exceptions.RequestException as e:
        # catch any timeouts etc here
        raise e
    geo_data = response.json()
    lat = geo_data[0]['lat']
    lon = geo_data[0]['lon']
    return [lat, lon]


if __name__ == "__main__":
    df = get_csv_data()

    # Start the display
    display = Display(visible=0, size=(1366, 768))
    display.start()

    # Get the data for each abstract link
    for idx, row in df.iterrows():
        df.at[idx, "abstract_text"] = get_abstract(url=row["abstract_link"])
        # Only do the first few for this test
        condition = ~pd.isnull(df["abstract_text"])
        if condition.sum() > 2:
            break

    # Tidy up
    display.stop()

    # Show off the results
    print(df.loc[condition])
