import requests                                          
import xml.etree.ElementTree as ET
         
TOP_URL = "https://gtr.ukri.org/gtr/api/projects"
TOTALPAGES_KEY = "{http://gtr.rcuk.ac.uk/gtr/api}totalPages"
PAGE_SIZE=100            
                    
def get_xml(**kwargs):                   
    r = requests.get(TOP_URL, params=kwargs)
    r.raise_for_status()                  
    root = ET.fromstring(r.text)                
    return root          
                                                 
projects = get_xml(p=1, s=PAGE_SIZE)
total_pages = int(root.attrib[TOTALPAGES_KEY])
                                                            
for page in range(1, total_pages+1):                        
    projects = get_xml(p=page, s=PAGE_SIZE)
    break



# def get_fund_data(page, config):
#     logging.info("Page %s", page)
#     r = requests.get(config["parameters"]["src"],
#                      params=dict(p=page, s=100))
#     # Assume that this means we're out of pages                   
#     if r.status_code != 200:
#         return False
        
#     # Append the organisations                                    
#     js = r.json()
#     funds = []
#     for row in js["fund"]:
#         # Copy the row, remove 'links', and flatten the dictionary
#         _row = row.copy()
#         _row.pop("links")
#         _row = flatten(_row)
#         _row["project_api_href"] = row["links"]["link"][1]["href"]
#         funds.append(_row.copy())
        
#     logging.info("\tGot %s funds.", len(funds))
#     return True
