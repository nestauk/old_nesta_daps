from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import boto3
import pandas as pd

s3 = boto3.resource('s3')
ELEMENT_EXISTS = EC.visibility_of_element_located((By.CSS_SELECTOR,
                                                   ("#dataViewTable > tbody > "
                                                    "tr.expanded-view.ng-scope > "
                                                    "td > div > div > p")))

def get_abstract(url):
    browser = webdriver.Chrome()
    browser.get(url)
    wait = WebDriverWait(browser, 60)
    element = wait.until(ELEMENT_EXISTS)
    text = element.text
    browser.quit()
    return text


def get_data():
    # Get the data
    obj = s3.Object("nesta-inputs", "world_reporter_inputs.csv")
    df = pd.read_csv(obj.get()['Body'])           
    df.columns = [col.replace(" ","_").lower() for col in df.columns]
    df["abstract_text"] = None
    return df
    
    
if __name__ == "__main__":

    df = get_data()
    
    # Fire up the browser
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
