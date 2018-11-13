import datetime
import logging
import requests
import time
import xml.etree.ElementTree as ET


OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV = "{http://arxiv.org/OAI/arXiv/}"
# N_PAPERS = 1385353
DELAY = 10  # seconds between requests
API_URL = 'http://export.arxiv.org/oai2'


# def get_file_number(n_papers):
#     # Then check if any files have been generated yet
#     file_numbers = []
#     for filename in os.listdir("data/"):
#         if not (filename.startswith("arxiv") and filename.endswith(".json")):
#             continue
#         file_number = int(filename.split("-")[1].split(".")[0])
#         file_numbers.append(file_number)
#     all_file_numbers = set(x for x in np.arange(0, n_papers, 1000))
#     return random.choice(list(all_file_numbers.difference(file_numbers)))

def arxiv_request(url, delay=DELAY, **kwargs):
    params = dict(verb='ListRecords', **kwargs)
    r = requests.get(url, params=params)
    xml = r.text
    time.sleep(delay)
    return ET.fromstring(xml)


def total_articles():
    """Make a request to the API and capture the total number of articles from the
    resumptionToken tag.

    Returns:
        (int): total number of articles
    """
    # params = dict(verb='ListRecords', metadataPrefix='arXiv')

    # r = requests.get(API_URL, params=params)
    # xml = r.text
    # root = ET.fromstring(xml)
    root = arxiv_request(API_URL, metadataPrefix='arXiv')
    token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    list_size = token.attrib['completeListSize']
    time.sleep(DELAY)
    return int(list_size)


def request_token():
    """Make a request to the API and capture the resumptionToken which will be used to
    page through results. Not efficient as it also returns the first page of data which
    is slow and not required.

    Returns:
        (string): supplied resumtionToken
    """
    root = arxiv_request(API_URL, metadataPrefix='arXiv')
    token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    seed_token = token.text.split("|")[0]
    logging.info(f"resumptionToken: {seed_token}")
    time.sleep(DELAY)
    return seed_token


def arxiv_batch(token, cursor):
    """Retrieves a batch of data from the arXiv api (expect 1000).

    Args:
        token (str): resumptionToken issued from a prevous request
        cursor (int): record to start from

    Returns:
        (:obj:`list` of :obj:`dict`): retrieved records
    """
    output = []
    resumptionToken = '|'.join([token, str(cursor)])
    params = dict(verb='ListRecords', resumptionToken=resumptionToken)

    r = requests.get('http://export.arxiv.org/oai2', params=params)
    xml = r.text
    root = ET.fromstring(xml)
    records = root.find(OAI+'ListRecords')

    for record in records.findall(OAI+"record"):
        header = record.find(OAI+'header')
        header_id = header.find(OAI+'identifier').text
        row = dict(datestamp=header.find(OAI+'datestamp').text)
        logging.info(f"article {header_id} datestamp: {row['datestamp']}")

        # collect all fields from the metadata section
        meta = record.find(OAI+'metadata')
        if meta is None:
            logging.warning(f"No metadata for article {header_id}")
            continue

        info = meta.find(ARXIV+'arXiv')
        fields = ['id', 'created', 'updated', 'title', 'categories',
                  'journal_ref', 'doi', 'msc-class', 'abstract']
        for field in fields:
            try:
                row[field] = info.find(ARXIV+field).text
            except AttributeError:
                logging.info(f"{field} not found in article {header_id}")

        # verify date fields are in the correct format
        date_fields = ['datestamp', 'created', 'updated']
        for field in date_fields:
            try:
                date = row[field]
                datetime.datetime.strptime(date, '%Y-%m-%d')
            except ValueError:
                logging.warning(f"{field}: {date} format is invalid")
                del row[field]
            except KeyError:
                # not found
                pass

        row['title'] = row['title'].strip



        # ***Authors

        # arxiv_id = info.find(ARXIV+'id').text
        # created = info.find(ARXIV+'created').text
        # created = datetime.datetime.strptime(created, '%Y-%m-%d')
            # updated = info.find(ARXIV+'updated').text
            # updated = datetime.datetime.strptime(updated, '%Y-%m-%d')
        # title = info.find(ARXIV+'title').text.strip()
        # categories = info.find(ARXIV+'categories').text  # space delimited...split
        # journal_ref = info.find(ARXIV+'journal-ref').text
        # doi = info.find(ARXIV+'doi').text
        # abstract = info.find(ARXIV+'abstract').text.strip()

        output.append(row)

    # Get next token                                                  
    # token = root.find(OAI+'ListRecords').find(OAI+"resumptionToken")
    # if token is None or token.text is None:
        # break
    # params = dict(verb='ListRecords', resumptionToken=token.text)   
    # Get cursor and write to file                                    
    # cursor = token.attrib['cursor']
    # filename = "data/arxiv-"+str(cursor)+".json"
    # d_n = len(all_ids) - n_ids_before
    # print(i_req, "Writing", d_n, "to", filename, "(", token.text, ")")
    # pd.DataFrame(output).to_json(filename, orient='records')
    time.sleep(DELAY)
# return True

# while not finished(N_PAPERS):
#     try:
#         get_arxiv_data()
#     except Exception:
#         print("Got",str(Exception))
#         time.sleep(25)
#         print("Restarting...")
#         print()


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        # level=logging.DEBUG,
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    token = get_token()
    get_arxiv_data(token, 0)

