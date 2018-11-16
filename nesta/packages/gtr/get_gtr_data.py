"""
Get GTR data
============

Extract all Gateway To Research data via
the official API, unpacking all data recursively through
project entities.
"""

import requests
import xml.etree.ElementTree as ET
import re
from collections import defaultdict
from datetime import datetime as dt

# Global constants
TOP_URL = "https://gtr.ukri.org/gtr/api/projects"
TOTALPAGES_KEY = "{http://gtr.rcuk.ac.uk/gtr/api}totalPages"
REGEX = re.compile(r'\{(.*)\}(.*)')
REGEX_API = re.compile(r'https://gtr.ukri.org:443/gtr/api/(.*)/(.*)')

def is_iterable(x):
    try:
        iter(x)
    except TypeError:
        return False
    else:
        return True
 

class TypeDict(dict):
    """dict-like class which converts string values to an
    appropriate type automatically."""
    def set_and_cast_item(self, k, v, _type):
        try:
            v = _type(v)
        except (ValueError, TypeError):
            return False
        super().__setitem__(k, v)
        return True

    def __setitem__(self, k, v):
        if v == {'nil': 'true'}:
            return super().__setitem__(k, None)
        # Don't bother if not a string
        if type(v) is not str:
            return super().__setitem__(k, v)
        # Try int and float
        if self.set_and_cast_item(k, v, int):
            return
        if self.set_and_cast_item(k, v, float):
            return
        # Try a couple of date formats
        fmt = '%Y-%m-%dT%H:%M:%SZ'
        if self.set_and_cast_item(k, v, lambda x: dt.strptime(x, fmt)):
            return
        fmt = '%Y-%m-%d%z'
        if self.set_and_cast_item(k, v, lambda x: dt.strptime(x.replace(":",""), fmt)):
            return
        # Otherwise, set as normal
        super().__setitem__(k, v)


def deduplicate_participants(data):
    """The participant data is a duplicate of organisation,
    with only two specific interesting fields. Unfortunately
    GtR doesn't use an easy naming convention which allows this
    procedure to be performed without hard-coding.

    Args:
        data (:obj:`defaultdict(list)`): Data holder, mapping entities to rows of data
    """
    # Iterate through participants
    for row in data.pop('participant'):
        # Find a matching organisation
        for _row in data['organisations']:
            if row['organisationId'] != _row['id']:
                continue
            for key in ('projectCost', 'grantOffer'):
                _row[key] = row[key]
            break


def unpack_funding(row):
    """Unpack and flatten data from any 'dict' field containing the
    "currencyCode" key.

    Args:
        row (dict): A row of GtR data to unpack.
                    Note, the row will be changed if any "text" keys are found.
    """
    to_pop = []
    for k, v in row.items():
        if type(v) is not dict:
            continue
        if "currencyCode" not in v:
            continue
        to_pop.append(k)
    for k in to_pop:
        fund_data = row.pop(k)
        for field, value in fund_data.items():
            row[field] = value


def unpack_list_data(list_items, data, project_id):
    """Unpack data from GtR "links.link" and "participant.participantValues" entities,
    and associate them with a project id.

    Args:
        list_items (:obj:`xml.etree.ElementTree`): A GtR "links" or "participantValues"entity.
        data (:obj:`defaultdict(list)`): Data holder, mapping entities to rows of data.
        project_id (str): Unique ID of the project associated with these entities.
    """
    key = list(list_items)[0]
    if type(list_items[key]) is not list:
        list_items[key] = [list_items[key]]
    for item in list_items[key]:
        item['project_id'] = project_id
        if 'entity' in item:
            key = item.pop('entity')
        unpack_funding(item)
        data[key].append(item)


def unpack_topics(row):
    """Any values stored under the key "text" in GtR are generally
    'topic'-like values, so unpack them as such.

    Args:
        row (dict): A row of GtR data to unpack.
                    Note, the row will be changed if any "text" keys are found.
    """
    topics = defaultdict(list)
    for k, v in row.items():
        # Case 1: the value is a list, so unpack from nested rows
        if type(v) is list:
            for nested_row in v:
                topic = unpack_topics(nested_row)
                # If anything has been found
                if topic is not None:
                    topics[k].append(topic)
        # Case 2: the value is a dict, so unpack directly
        elif type(v) is TypeDict:
            # Return the topic if it exists
            try:
                return v['text']
            # Otherwise, keep trying to unpack
            except KeyError:
                topic = unpack_topics(v)
                if topic is not None:
                    topics[k].append(topic)
    # Finally, flatten the topics into the row, replacing
    # any fields that existed before. The design choice for
    # the storing this as a joined 'str' is not one I'm proud of,
    # but it suits our downstream schema.
    for k, v in topics.items():
        row[k] = "|".join(v)


def extract_link_data(url):
    """Enter a link URL and recursively extract the data.

    Args:
        url (str): A GtR URL from which to extract data.
    Returns:
        row (dict): Unpacked GtR data.
    """
    et = read_xml_from_url(url)
    row = TypeDict()
    # Note: Ignore any links and hrefs, as this will lead to
    # infinite recursion!
    extract_data_recursive(et, row, ignore=['links', 'href'])
    return row


def extract_data(et, ignore=[]):
    """Generically extract and flatten any GtR entity.

    Args:
        et (:obj:`xml.etree.ElementTree`): A GtR XML entity "row".
        ignore (:obj:`list` of :obj:`str`): Ignore any entity types in this list.
    Returns:
        entity, row (str, dict): Entity type and data.
    """
    row = TypeDict()
    # Get the root entity name
    _, entity = REGEX.findall(et.tag)[0]
    if entity in ignore:
        return entity, row
    # Iterate over data fields
    for k, v in et.attrib.items():
        # Extract the field name
        _, field = REGEX.findall(k)[0]
        if field in ignore:
            continue
        # URLs need to be treated differently, as they point to new data
        if field == 'href':
            # Get the ID and entity type of the object pointed to by the URL
            _entity, _id = REGEX_API.findall(v)[0]
            # ... then extract the data at that URL
            _entity_data = extract_link_data(v)
            # Finally, unpack the data as usual
            row['entity'] = _entity
            row['id'] = _id
            for _k, _v in _entity_data.items():
                row[_k] = _v
        # If the field appears multiple times, ignore it
        elif field in row:
            continue
        # Otherwise, this just is a simple flat field
        else:
            row[field] = v
    # If there is any text data, unpack it into a fake field called "value"
    if et.text not in (None, ''):
        row["value"] = et.text
    # If this row contains "value", and nothing else, then flatten it further
    if "value" in row and len(row) == 1:
        row = row["value"]
    return entity, row


def extract_data_recursive(et, row, ignore=[]):
    """Recursively dive into and extract a row of data.

    Args:
        et (:obj:`xml.etree.ElementTree`): A GtR XML entity "row".
        row (dict): The output row of data to fill.
        ignore: See :obj:`extract_data`.
    """
    for c in et.getchildren():
        # Extract the shallow data for this row
        entity, _row = extract_data(c, ignore)
        if entity in ignore:
            continue
        # Unpack any deep data into the shallow _row that we just extracted
        extract_data_recursive(c, _row, ignore)
        # Ignore duplicate entries
        if entity in row and _row == row[entity]:
            continue
        # Treat 'link' objects differently: append as a list item to the parent row
        if entity in row and entity in ('link', 'participant'):
            if type(row[entity]) is not list:
                row[entity] = [row[entity]]
            row[entity].append(_row)
        # Otherwise, append any non-empty data to the parent row
        elif (not is_iterable(_row)) or len(_row) > 0:
            row[entity] = _row


def read_xml_from_url(url, **kwargs):
    """Read pure XML data directly from a URL.

    Args:
        url (str): The source URL.
        kwargs (dict): Any :obj:`params` data to pass to :obj:`requests.get`.
    Returns:
        An `:obj:`xml.etree.ElementTree` of the full XML tree.
    """
    r = requests.get(url, params=kwargs)
    r.raise_for_status()
    et = ET.fromstring(r.text)
    return et


if __name__ == "__main__":

    # Local constants
    PAGE_SIZE=100

    # Assertain the total number of pages first
    projects = read_xml_from_url(TOP_URL, p=1, s=PAGE_SIZE)
    total_pages = int(projects.attrib[TOTALPAGES_KEY])

    # The output data structure:
    # each key represents a unique flat entity (i.e. a flat 'table')
    # each list represents rows in that table.
    data = defaultdict(list)

    # Iterate through all pages
    for page in range(1, total_pages+1):
        # Get all projects on this page
        projects = read_xml_from_url(TOP_URL, p=page, s=PAGE_SIZE)
        for project in projects.getchildren():
            # Extract the data for the project into 'row'
            _, row = extract_data(project)
            # Then recursively extract data from nested rows into the parent 'row'
            extract_data_recursive(project, row)
            # Flatten out any list data directly into 'data' under separate tables
            unpack_list_data(row.pop('links'), data, row['id'])
            if 'participantValues' in row:                
                unpack_list_data(row.pop('participantValues'), data, row['id'])
            # Flatten out any topic-like data (lists of strings) into the parent 'row'
            unpack_topics(row)
            # Flatten out any nested funding information into the parent 'row'
            unpack_funding(row)
            # 'identifiers' is the only very uninteresting data and it also
            # mucks up the flat schema
            row.pop('identifiers')
            # Append the row
            data[row.pop('entity')].append(row)
            break
        break

    # The 'participant' data is near duplicate
    # of 'organisation' data so merge them.
    deduplicate_participants(data)
    print(data)
