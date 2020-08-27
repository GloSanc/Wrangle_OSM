#!/usr/bin/env python
# coding: utf-8

# File content:
# 
# Touluse CSV creation: Code for correct the street name and create the CSV files.
# 
# Toulouse database and SQL queries.
# 
# TOULOUSE - FRANCE IN OSM.
# Postal codes 31000, 31100, 31200,31300, 31400 and 31500 + CEDEX (those are the postal codes of Toulouse urban area).
# The Coordinates we will extract are: 43.6552N, 1.5024E, 43.5595S, 1.3935W

# In[1]:


# This is the list of imports we are going to use

import xml.etree.ElementTree as ET
from collections import defaultdict
import re
import pprint
import os
import os.path
import sys
import time
import requests
import codecs
import json
from IPython.display import Image
import csv
import codecs
import sqlite3


# We define the corrections to be pass when CSV creation

# In[13]:


#Function to correct street name when creating CSV files

dataset = 'UdcT2'

street_type_re = re.compile(r'(?P<word>)(\b\w+\b)', re.IGNORECASE)

street_types = defaultdict(int)

#List of expected street types

expected = ['Rue', 'Allée', 'Avenue',  'Place', 'Impasse', 'Route', 'Chemin', 'Boulevard', 
            'Allées', 'Esplanade', 'Port', 'Promenade', 'Quai', 'Passage', 'Cheminement', 
            'Voie', 'Descente', 'Square', 'Contre-Allée','Périphérique', 'Cours', 'Parvis', 
            'Grande','Angle','Sur','face']

#Dictionary to correct the capitals missing in street type
cap_mapping = { 'route' :'Route',
            'ROUTE' :'Route',
            'rue' : 'Rue' ,
            'AVENUE': 'Avenue',
            'avenue': 'Avenue',
            'place':  'Place' ,
            'allées' :'Allées',
            'allée': 'Allée',
            'voie' :'Voie',
           'chemin':'Chemin'
                }

# Dictionary to correct the wrong naming of street
other_mapping = {'107':'Cours Rosalind Franklin, 107',
         '6' :'Impasse Leonce Couture, 6' ,
           '9': 'Rue Reclusane, 9'  ,
          'Frédéric':'Rue Frédéric Petit',
          'Lotissement':'Impasse René Mouchotte_Lotissement Futuropolis'
               }

# Fucntion to pring a dict sorted by key
def print_sorted_dict(d, expression):
    keys = d.keys()
    keys = sorted(keys, key=lambda s: s.lower())
    for k in keys:
        v = d[k]
        print (expression % (k, v))

# Function to find the street in the file
def is_street_name(elem):
    return (elem.tag == "tag") and (elem.attrib['k'] == "addr:street")

# Function to find the street with type not in expected type
def expected_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


# Function to find the street with not expected street type in the file
def audit_st_tp(filename):
    problem_street_types = defaultdict(set)
    for event, elem in ET.iterparse(filename):
        if is_street_name(elem):
            expected_street_type(problem_street_types, elem.attrib['v'])
    return problem_street_types

#Function to define the correct streets name with wrong naming
def other_correct(street_name, street_type):
    if type(other_mapping[street_type]) == type('string'):
        name = other_mapping[street_type]
    else:
        for key in other_mapping[street_type]:
                name = key
    return name

# Function to correct the names of the street        
def update_name(name):
    street_type= name.split(' ',1)[0]
    street_name= name.split(' ',1)[-1]        

    if street_type in cap_mapping:
        name = cap_mapping[street_type] + ' ' + street_name  
    elif street_type in other_mapping:
        name = other_correct(street_name, street_type)
    return name


#Function to create the dictionary that we will use to change the names of the streets when creating CSV
def run_updates(filename):
    st_types = audit_st_tp(dataset)
    for st_type, ways in st_types.items():
        for name in ways:
            better_name = update_name(name) # voy a prograr update name
            if better_name != name:
                corrected_names[name] = better_name
    return corrected_names

#Create the dictionary
corrected_names = {}   
corrected_names = run_updates(dataset)

#Print the correct names
print_sorted_dict(corrected_names, "%s: %s")  


# In[14]:


# This is the schema for the CSV creation

SCHEMA = {
    'node': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'lat': {'required': True, 'type': 'float', 'coerce': float},
            'lon': {'required': True, 'type': 'float', 'coerce': float},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'node_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    },
    'way': {
        'type': 'dict',
        'schema': {
            'id': {'required': True, 'type': 'integer', 'coerce': int},
            'user': {'required': True, 'type': 'string'},
            'uid': {'required': True, 'type': 'integer', 'coerce': int},
            'version': {'required': True, 'type': 'string'},
            'changeset': {'required': True, 'type': 'integer', 'coerce': int},
            'timestamp': {'required': True, 'type': 'string'}
        }
    },
    'way_nodes': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'node_id': {'required': True, 'type': 'integer', 'coerce': int},
                'position': {'required': True, 'type': 'integer', 'coerce': int}
            }
        }
    },
    'way_tags': {
        'type': 'list',
        'schema': {
            'type': 'dict',
            'schema': {
                'id': {'required': True, 'type': 'integer', 'coerce': int},
                'key': {'required': True, 'type': 'string'},
                'value': {'required': True, 'type': 'string'},
                'type': {'required': True, 'type': 'string'}
            }
        }
    }
}


# In[15]:


# Creation of CSV 

dataset = 'UdcT2'

NODES_PATH = "nodesT.csv"
NODE_TAGS_PATH = "nodes_tagsT.csv"
WAYS_PATH = "waysT.csv"
WAY_NODES_PATH = "ways_nodesT.csv"
WAY_TAGS_PATH = "ways_tagsT.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']

# Funtion to correct the street
def correct_element(v):
    if v in corrected_names:
        correct_value = corrected_names[v]
    else:
        correct_value = v
    return correct_value

#Funntion to define nodes and ways
def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  

    if element.tag == 'node':
        node_attribs['id'] = element.attrib['id']
        node_attribs['user'] = element.attrib['user']
        node_attribs['uid'] = element.attrib['uid']
        node_attribs['version'] = element.attrib['version']
        node_attribs['lat'] = element.attrib['lat']
        node_attribs['lon'] = element.attrib['lon']
        node_attribs['timestamp'] = element.attrib['timestamp']
        node_attribs['changeset'] = element.attrib['changeset']
        
        for node in element:
            tag_dict = {}
            tag_dict['id'] = element.attrib['id']
            if ':' in node.attrib['k']:
                tag_dict['type'] = node.attrib['k'].split(':', 1)[0]
                tag_dict['key'] = node.attrib['k'].split(':', 1)[-1]
                tag_dict['value'] = correct_element(node.attrib['v'])
            else:
                tag_dict['type'] = 'regular'
                tag_dict['key'] = node.attrib['k']
                tag_dict['value'] = correct_element(node.attrib['v'])
            tags.append(tag_dict)
            
    elif element.tag == 'way':
        way_attribs['id'] = element.attrib['id']
        way_attribs['user'] = element.attrib['user']
        way_attribs['uid'] = element.attrib['uid']
        way_attribs['version'] = element.attrib['version']
        way_attribs['timestamp'] = element.attrib['timestamp']
        way_attribs['changeset'] = element.attrib['changeset']
        n = 0
        for node in element:
            if node.tag == 'nd':
                way_dict = {}
                way_dict['id'] = element.attrib['id']
                way_dict['node_id'] = node.attrib['ref']
                way_dict['position'] = n
                n += 1
                way_nodes.append(way_dict)
            if node.tag == 'tag':
                tag_dict = {}
                tag_dict['id'] = element.attrib['id']
                if ':' in node.attrib['k']:
                    tag_dict['type'] = node.attrib['k'].split(':', 1)[0]
                    tag_dict['key'] = node.attrib['k'].split(':', 1)[-1]
                    tag_dict['value'] = correct_element(node.attrib['v'])
                else:
                    tag_dict['type'] = 'regular'
                    tag_dict['key'] = node.attrib['k']
                    tag_dict['value'] = correct_element(node.attrib['v'])
                tags.append(tag_dict)
    
    if element.tag == 'node':
        return {'node': node_attribs, 'node_tags': tags}
    elif element.tag == 'way':
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #

def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""
    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
            k: (v.encode('utf-8') if isinstance(v, str) else v) for k, v in row.items()
        })
    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w',encoding="utf-8") as nodes_file,     codecs.open(NODE_TAGS_PATH, 'w',encoding='utf-8') as nodes_tags_file,     codecs.open(WAYS_PATH, 'w',encoding='utf-8') as ways_file,     codecs.open(WAY_NODES_PATH, 'w',encoding='utf-8') as way_nodes_file,     codecs.open(WAY_TAGS_PATH, 'w',encoding='utf-8') as way_tags_file:

        nodes_writer = csv.DictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = csv.DictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = csv.DictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = csv.DictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = csv.DictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow((el['way']))
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])
                    
process_map(dataset)


# We have now the CSV file created, we can create our database

# In[5]:


# Creating database on disk

sqlite_file = 'UdcTls.db'

conn = sqlite3.connect(sqlite_file)

c = conn.cursor()

c.execute('''DROP TABLE IF EXISTS nodes''')
c.execute('''DROP TABLE IF EXISTS nodes_tags''')
c.execute('''DROP TABLE IF EXISTS ways''')
c.execute('''DROP TABLE IF EXISTS ways_tags''')
c.execute('''DROP TABLE IF EXISTS ways_nodes''')
conn.commit()



QUERY_NODES = """
CREATE TABLE nodes (
    id INTEGER NOT NULL,
    lat REAL,
    lon REAL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT
);
"""

QUERY_NODES_TAGS = """
CREATE TABLE nodes_tags (
    id INTEGER,
    key TEXT,
    value TEXT,
    type TEXT,
    FOREIGN KEY (id) REFERENCES nodes(id)
);
"""

QUERY_WAYS = """
CREATE TABLE ways (
    id INTEGER NOT NULL,
    user TEXT,
    uid INTEGER,
    version INTEGER,
    changeset INTEGER,
    timestamp TEXT
);
"""

QUERY_WAYS_TAGS = """
CREATE TABLE ways_tags (
    id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    type TEXT,
    FOREIGN KEY (id) REFERENCES ways(id)
);
"""

QUERY_WAYS_NODES = """
CREATE TABLE ways_nodes (
    id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    position INTEGER NOT NULL,
    FOREIGN KEY (id) REFERENCES ways(id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);
"""



c.execute(QUERY_NODES)
c.execute(QUERY_NODES_TAGS)
c.execute(QUERY_WAYS)
c.execute(QUERY_WAYS_TAGS)
c.execute(QUERY_WAYS_NODES)

conn.commit()


# In[6]:


#Creating structure of database

with open('nodes.csv','rt', encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db1 = [(i['id'], i['lat'], i['lon'], i['user'], i['uid'], i['version'], i['changeset'], i['timestamp']) for i in dr]
    
with open('nodes_tags.csv','rt',encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db2 = [(i['id'], i['key'], i['value'], i['type']) for i in dr]
    
with open('ways.csv','rt', encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db3 = [(i['id'], i['user'], i['uid'], i['version'], i['changeset'], i['timestamp']) for i in dr]
    
with open('ways_tags.csv','rt', encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db4 = [(i['id'], i['key'], i['value'], i['type']) for i in dr]
    
with open('ways_nodes.csv','rt', encoding='utf8') as fin:
    dr = csv.DictReader(fin) # comma is default delimiter
    to_db5 = [(i['id'], i['node_id'], i['position']) for i in dr]


# In[7]:


#Filling the database

c.executemany("INSERT INTO nodes(id, lat, lon, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?);", to_db1)
c.executemany("INSERT INTO nodes_tags(id, key, value, type) VALUES (?, ?, ?, ?);", to_db2)
c.executemany("INSERT INTO ways(id, user, uid, version, changeset, timestamp) VALUES (?, ?, ?, ?, ?, ?);", to_db3)
c.executemany("INSERT INTO ways_tags(id, key, value, type) VALUES (?, ?, ?, ?);", to_db4)
c.executemany("INSERT INTO ways_nodes(id, node_id, position) VALUES (?, ?, ?);", to_db5)
conn.commit()


# In[12]:


# Query to count the nodes and ways in database
c.execute('SELECT COUNT(*) FROM nodes')
all_rows = c.fetchall()
print('Nodes in the data base:', all_rows)

c.execute('SELECT COUNT(*) FROM ways')
all_rows = c.fetchall()
print('Ways in data base:',all_rows)


# In[39]:


# Query to show the nicknames *user* and contributions of the top 15 contributors
QUERY = '''
SELECT DISTINCT nodes.user, COUNT(*)
FROM nodes
GROUP BY nodes.user
ORDER BY COUNT(*) DESC
LIMIT 15;
'''
c.execute(QUERY)
all_rows = c.fetchall()
print('Top 15 contributors and their contributions:',all_rows)


# In[40]:


# Query to show nicknames *user* and contributions of the top 10 contributors in %
QUERY = '''
SELECT DISTINCT nodes.user, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes)
FROM nodes
GROUP BY nodes.uid
ORDER BY (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes)) DESC
LIMIT 10;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Top 10 contributors and the % of their contributions:', all_rows)


# In[48]:


# Query to see the source of information
QUERY = '''
SELECT value, COUNT(*) as Count
FROM nodes_tags
WHERE key='source'
GROUP BY value
ORDER BY Count DESC
LIMIT 15;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Top 15 source of informations:',all_rows)


# In[43]:


# Query to show % of information by source 
QUERY = '''
SELECT value, COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes_tags)
FROM nodes_tags
WHERE key='source'
GROUP BY value
ORDER BY (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nodes)) DESC
LIMIT 10;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Top 10 source of informations in %:',all_rows)


# In[24]:


# Query to count the tags with more nodes, show top 10
QUERY = '''
SELECT ways_tags.value, COUNT(*)
FROM ways_tags
WHERE ways_tags.key = 'name'
AND ways_tags.type = 'regular'
GROUP BY ways_tags.value
ORDER BY COUNT(*) DESC
LIMIT 10;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print(all_rows)


# In[15]:


# Query to count the top 15 amenities
QUERY = '''
SELECT value, COUNT(*) as Count
FROM nodes_tags
WHERE key='amenity'
GROUP BY value
ORDER BY Count DESC
LIMIT 15;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print(all_rows)


# If after reading this project you decide to come to visit Touluse you will have enouth place to eat with 726 restaurant, 356 fast-food, 178 bar and 172 cafe
# And if you need money you can go to one of the 157 banks
# You can rent a bicycle in one of the 286 rental point
# Or send a postcard to your friends from one of the 279 post box
# If after all that you are tyred you can set in one fo the 985 bench that are spread in the city

# In[28]:


# Query to count the cuisine of the restaurant
QUERY = '''
SELECT nodes_tags.value, COUNT(*) as count
FROM nodes_tags 
JOIN
    (SELECT DISTINCT(id)
    FROM nodes_tags
    WHERE value='restaurant') as Sub
ON nodes_tags.id=Sub.id
WHERE nodes_tags.key='cuisine'
GROUP BY nodes_tags.value
ORDER BY Count DESC;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print(all_rows)


# When we check the type of cuisien we have a winner: the French cuisine, what such surprise!!!
# But if you finaly come, I recomend you to taste the local cuisine in one of the 29 regional restaurant

# In[73]:


# Query to see the brands of fast-food 
QUERY = '''
SELECT nodes_tags.value, COUNT(*) as count
FROM nodes_tags 
JOIN
    (SELECT DISTINCT(id)
    FROM nodes_tags
    WHERE value='fast_food') as Sub
ON nodes_tags.id=Sub.id
WHERE nodes_tags.key='brand'
GROUP BY nodes_tags.value
ORDER BY Count DESC;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Do yo prefer fast-food, we have some too:',all_rows)


# In[53]:


# Query to count the top 10 shop types
QUERY = '''
SELECT value, COUNT(*) as Count
FROM nodes_tags
WHERE key='shop'
GROUP BY value
ORDER BY Count DESC
LIMIT 10;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Do you want to go shopping?',all_rows)


# In[74]:


# Query to find the normal time the mail is colected in the post office
QUERY = '''
SELECT value, COUNT(*) as Count
FROM nodes_tags
WHERE key='collection_times'
GROUP BY value
ORDER BY Count DESC
LIMIT 15;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('You want to know at what time the postcard for your friend will be collected:',all_rows)


# In[68]:


# Query to find the type of sport places
QUERY = '''
SELECT value, COUNT(*) as Count
FROM nodes_tags
WHERE key='sport'
GROUP BY value
ORDER BY Count DESC
LIMIT 15;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Now some sport:',all_rows)


# We have 4 places for climbing, as we need to be trainned for our trips to Pyrinees

# In[71]:


# Query to find the tourism things 
QUERY = '''
SELECT value, COUNT(*) as Count
FROM nodes_tags
WHERE key='tourism'
GROUP BY value
ORDER BY Count DESC
LIMIT 15;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('But you come for tourism, dont you?:',all_rows)


# In[77]:


# Query to see the brands of fast-food 
QUERY = '''
SELECT nodes_tags.value, COUNT(*) as count
FROM nodes_tags 
JOIN
    (SELECT DISTINCT(id)
    FROM nodes_tags
    WHERE value='hotel') as Sub
ON nodes_tags.id=Sub.id
WHERE nodes_tags.key='name'
GROUP BY nodes_tags.value
ORDER BY Count DESC;
'''

c.execute(QUERY)
all_rows = c.fetchall()
print('Now you need to find a place for the night, you can chose amoung those:',all_rows)


# And that is all about my city, hope you enjoy the trip.
