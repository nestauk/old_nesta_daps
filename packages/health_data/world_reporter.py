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
import boto3
import pandas as pd

s3 = boto3.resource('s3')
# Path to the abstract element
ELEMENT_EXISTS = EC.visibility_of_element_located((By.CSS_SELECTOR,
                                                   ("#dataViewTable > tbody > "
                                                    "tr.expanded-view.ng-scope > "
                                                    "td > div > div > p")))

def get_abstract(url):
    '''Wait for the abstract to appear at the URL

    Args:
        url (str): An abstract URL specified from the input CSV file.
    '''
    # Fire up a browser
    browser = webdriver.Chrome()
    browser.get(url)
    # Wait until the element appears
    wait = WebDriverWait(browser, 60)
    element = wait.until(ELEMENT_EXISTS)
    text = element.text
    # Tidy up and exit
    browser.quit()
    return text


def get_csv_data():
    '''Retrieve the CSV data from AWS and tidy the columns'''
    obj = s3.Object("nesta-inputs", "world_reporter_inputs.csv")
    df = pd.read_csv(obj.get()['Body'])           
    df.columns = [col.replace(" ","_").lower() for col in df.columns]
    # Create a field ready for the abstract text
    df["abstract_text"] = None
    return df
    
    
if __name__ == "__main__":
    df = get_csv_data()
    
    # Start the display
    display = Display(visible=0, size=(1366, 768))
    display.start()

    # Get the data for each abstract link
    for idx, row in df.iterrows():
        df.at[idx, "abstract_text"] = get_abstract(browser, url=row["abstract_link"])
        # Only do the first few for this test
        condition = ~pd.isnull(df["abstract_text"])
        if condition.sum() > 2:
            break
            
    # Tidy up
    display.stop()

    # Show off the results
    print(df.loc[condition])
