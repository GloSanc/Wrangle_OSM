#!/usr/bin/env python
# coding: utf-8

# File content
# Tls_wrangling: Code for download and process dataset: search, find, solve
# TOULOUSE - FRANCE IN OSM
# Postal codes 31000, 31100, 31200,31300, 31400 and 31500 + CEDEX (those are the postal codes of Toulouse urban area)
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


# We can download directly from the OSM page and open the file once downloaded from OSM via API or
# we can use a functions to download the data from openstreetmap passing as arguments the url and the path and name for our file. 
# The coordinates of the area should be as as West, South, East, North.
# We include in the fucntion some code to know the size of the file too.
# We want to know how long it takes the download, we add some code for that.

# In[3]:


#Open the file once downloaded from OSM via API
osm_Tls1 = open("Tls_Udc_map", "r", encoding='utf-8')


# In[2]:


#Function to download the map directly form OSM
def download_file(api_url, local_filename):
    r = requests.get(api_url, stream=True)
    with open(local_filename, 'wb') as f:
         for chunk in r.iter_content(chunk_size=4096):
            if chunk:
                f.write(chunk)
             
    file_size=   (os.path.getsize(local_filename))
    size_GB=round( file_size/(1024*1024) ,3)
    print ('\nDownload finished. {} is ready.'.format(local_filename))
    print (os.path.getsize(local_filename), 'bytes', size_GB, 'GB')


# In[3]:


#Attention: The coordinates of the area should be as as West, South, East, North.
start_time = time.time()
#This download the complete Toulouse map
url = 'http://overpass-api.de/api/map?bbox=1.3935, 43.5595,1.5024,43.6552'#indicate here the coordinate
osm_Tls0 = 'UdcT0'

# This download the sample map
#url = 'http://overpass-api.de/api/map?bbox=1.4224, 43.6126,1.4354,43.6248'

download_file(url, osm_Tls0)

time_speend=(time.time() - start_time)

print ('Time spend on downloading:', round(time_speend,0),'sec')


# We extarct an square area but the postal code in France does not delimite a square area, then we need to extract an area that covers that postal code and then filter it.
# 

# In[9]:


# Function to investigate the postal code in the map 
def count_postcodes(filename):
    postcodes = {}
    for event, elem in ET.iterparse(filename, events=('start', 'end')):
        if event == 'end':
            key = elem.attrib.get('k')
            if key == 'addr:postcode':
                postcode = elem.attrib.get('v')
                if postcode not in postcodes:
                    postcodes[postcode] = 1
                else:
                    postcodes[postcode] += 1
    return postcodes


postcodes = count_postcodes(osm_Tls0)
sorted_by_occurrence = [(k, v) for (v, k) in sorted([(value, key) for (key, value) in postcodes.items()], reverse=True)]
print ('Postal codes values and occurrence in download file from Toulouse:\n')
pprint.pprint(sorted_by_occurrence)


# We see the postal codes and we found more than expected.
# We check the numbers in https://www.laposte.fr/particulier/outils/trouver-un-code-postal that is the oficial page with the postal code information.
# We see some codes that could be a mistake :  ('31200\u200e', should be 31200 then we do not delete but correct it),  ('3140'should be 31400 then we do not delete but correct it), ('68199' should be 31000 then we do not delete but correct it).
# We found that main of the codes with 1 ocurrency correspond to the system known as CEDEX: Courrier d'Entreprise à Distribution EXceptionnelle ("business mail with special delivery"), designed for recipients of large volumes of mail. Those codes are correct as belong to Toulouse
# We found the codes '31130','31240','31700', '31390', '31140' that we need to eliminate as does not correspond to Toulouse. 
# 
# Now we clean the file to get the perimetre we want to investigate 

# In[11]:


# Function: to delete the elem that have postal code not in Toulouse area 
def get_postcode(elem):
    if elem.tag in ['node', 'way', 'relation']:
        for tag in elem.iter():
            if tag.get('k') == 'addr:postcode':
                return True, tag.get('v')
        return False, None
    return False, None

def clean_postcode(filename, cleaned_filename):
    tree = ET.parse(filename)
    root = tree.getroot()
    
    for child in ['node', 'way', 'relation']:
        for elem in root.findall(child):
            has_postcode, postcode_value = get_postcode(elem)
            if has_postcode:
                if postcode_value in ['31130','31240','31700', '31390', '31140']:
                    root.remove(elem)
    
    return tree.write(cleaned_filename)

def count_postcodes(filename):
    postcodes = {}
    for event, elem in ET.iterparse(filename, events=('start', 'end')):
        if event == 'end':
            key = elem.attrib.get('k')
            if key == 'addr:postcode':
                postcode = elem.attrib.get('v')
                if postcode not in postcodes:
                    postcodes[postcode] = 1
                else:
                    postcodes[postcode] += 1
    return postcodes


# In[12]:


#We clean the postal codes, create a file with right perimetre and get the list of postal codes

Tls_0 = 'UdcT2'

clean_postcode(osm_Tls0, Tls_0)

PC_Tls = count_postcodes(Tls_0)
PC_Tls_by_occurrence = [(k, v) for (v, k) in sorted([(value, key) for (key, value) in PC_Tls.items()], reverse=True)]

print ('Postcode values and occurrence in Touluse city:\n')
pprint.pprint(PC_Tls_by_occurrence)


# The file now have the right perimetre

# Now we have to analize if we have issues in the data before to transfer them to a database for our analysis and what kind of data we have in the file ex. tags, nodes, members.
# We use a function to count the tags

# Lets start checking the "k" value for each "tag" and see if there are any potential problems

# In[29]:


# Funtion to check if issues in 'k' value for 'tag'
# Focus on lower case, tag with underscore or lower_colon), problematic characters
# as: ?, %, #, $, @ and other posible characteres

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

def key_type(element, keys):
    if element.tag == "tag":
        if lower.search(element.attrib['k']):
            keys['lower'] += 1
        elif lower_colon.search(element.attrib['k']):
            keys['lower_colon'] += 1
        elif problemchars.search(element.attrib['k']):
            keys['problemchars'] += 1
        else:
            keys['other'] += 1

    return keys 

def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys


# In[30]:


# We pass the function to Tls_0
keys = process_map(Tls_0)
pprint.pprint(keys)


# We found only one problem. It seams that the quality of this data set is very good 
# Now lets find the problem and solve it

# In[33]:


# Funtion to get the element that generate the problem
def get_problemkeys(filename):
    """
    Takes in a dataset in XML format, parses it and returns a list with the values of tags with problematic characters.
    """
    problemchars_list = []
    for _, element in ET.iterparse(filename):
        if element.tag == 'tag':
            if problemchars.search(element.attrib['k']):
                problemchars_list.append(element.attrib['k'])
    return problemchars_list


# In[34]:


# We pass the function to Tls_0
print(get_problemkeys(Tls_0))


# The issue is not and issue as the point is an space between two words. We do not need to correct it.
# Now we analyse the nodes, tag, members ... of the file 

# In[21]:


# Funtion to show the tags and how many of them
def count_tags(filename):
    tags = {}
    for event, elem in ET.iterparse(filename, events=('start', )):
        if elem.tag not in tags:
            tags[elem.tag] = 1
        else:
            tags[elem.tag] += 1
    return tags

#Funtion to get the attributes and their amounts
def count_attrs(filename):
    attrs = {}
    for event, elem in ET.iterparse(filename, events=('start', 'end')):
        if event == 'end':
            for attr in elem.attrib:
                if attr not in attrs:
                    attrs[attr] = 1
                else:
                    attrs[attr] += 1
    return attrs


# In[15]:


# Iterative parsing to process the file with right perimetre Tls_0

tags_Tls = count_tags(Tls_0)
tags_Tls_by_occurrence = [(k, v) for (v, k) in sorted([(value, key) for (key, value) in tags_Tls.items()], reverse=True)]

print ('Element types and occurrence of Toulouse city map: \n')
pprint.pprint(tags_Tls_by_occurrence)


# Lets make now a view of atribures on the file

# In[22]:


# We pass the functions to Tls_0 and we calculate the time to pass this, as big database take time to pass
start_time1 = time.time()
attrs = count_attrs(Tls_0)
sorted_by_occurrence = [(k, v) for (v, k) in sorted([(value, key) for (key, value) in attrs.items()], reverse=True)]

print ('Attributes and occurrence on Toulouse city map:\n')
pprint.pprint(sorted_by_occurrence)

time_speend1=(time.time() - start_time1)
print ('Time spend on running:', round(time_speend1,0),'sec')


# We could check now the contributors to the map.

# In[23]:


#Funtions to get how many of uids and users 

def process_map_uid(filename):
    uids = set()
    for _, element in ET.iterparse(filename):
        if 'uid' in element.attrib :
            uids.add(element.attrib['uid'])
    return uids

def process_map_user(filename):
    users = set()
    for _, element in ET.iterparse(filename):
        if 'user' in element.attrib :
            users.add(element.attrib['user'])
    return users


# In[24]:


# We pass the functions to Tls_0
T_uids=process_map_uid(Tls_0)
T_users=process_map_user(Tls_0)


# In[25]:


# We see the amount of contributors in Tls_0: ids and name 
print(len(T_uids),'contributor number',len(T_users),'contributor Nickname')


# The number of contributors is the same for uid and user, we do not see any issue on the user view.
# We could get the information for 'uid' or 'user'. The uid never change but the contributors could decide to change their 'user' (kind of nickname). Existing elements will reflect the new user name without needing any version change.

# Each contributor is a person that spend time to contribure to the map, let see how many contributors are and how many contributions, and the ratio of contributions per user.

# In[26]:


# Funtions to get information about the contributors


def process_map(filename):
    users = set()
    for _, element in ET.iterparse(filename):
        if 'uid' in element.attrib :
            users.add(element.attrib['uid'])
    return users

#Funtion to get the name (Nickmane) 
def count_name_users(filename):
    many_name_users = {}
    for _, element in ET.iterparse(filename):
        if 'user' in element.attrib :
            many_name_user  = element.attrib.get('user')
            if many_name_user not in many_name_users:
                many_name_users[many_name_user] = 1
            else:
                many_name_users[many_name_user] += 1
                
    return many_name_users

#Funtion to get the amount of contribution by name (Nickmane) 
def count_rib(filename):
    cont=0
    for _, element in ET.iterparse(filename):
        if 'user' in element.attrib :
            cont= cont+1
    return cont


# In[35]:


# We pass the function to Tls_0

name_users = count_name_users(Tls_0)
num_users = count_rib(Tls_0)

print(len(name_users),'contributors and',num_users,'cotributions')
print(round((num_users/(len(name_users))),0),'contribution per contribtor as average')


# Lets check now the names of the "street". France street name start with the type of street and then the name.
# We need to take that into acount for the expresion to be included in the function for street type

# In[37]:


# Functions to get the type of street 

street_type_re = re.compile(r'(?P<word>)(\b\w+\b)', re.IGNORECASE)
street_types = defaultdict(int)

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        street_types[street_type] += 1

def print_sorted_dict(d, expression):
    keys = d.keys()
    keys = sorted(keys, key=lambda s: s.lower())
    for k in keys:
        v = d[k]
        print (expression % (k, v))

def is_street_name(elem):
    return (elem.tag == "tag") and (elem.attrib['k'] == "addr:street")

def check_street(filename):
    for event, elem in ET.iterparse(filename):
        if is_street_name(elem):
            audit_street_type(street_types, elem.attrib['v'])
    print(street_types, "%s: %d")
    return(street_types)


# In[38]:


# We pass the functions to Tls_0 and check the type of street
all_types = check_street(Tls_0)


# We see the rigth names of street that we will use to create our expected values list, we have Capitals/non capitals and there are other types that we need to investigate as seam not right. 
# 
# Remark:
# 'Allée' and 'Allées' are both correct 
# 
# We create a list of mapping to correct Capitals/non capitals: 'route','rue', 'AVENUE', 'avenue', 'place', 'ROUTE', 'allées', 'allée', 'voie', 'Périphérique'
# 
# We investigate for the wrong naming: 'Grande', 'face', 'Sur','107', 'Frédéric', '6', '9', 'Angle'Lotissement'
# 

# In[41]:


#Functions to audit the street names and investigate that seams not right

street_type_re = re.compile(r'(?P<word>)(\b\w+\b)', re.IGNORECASE)

expected = ['Rue', 'Allée', 'Avenue',  'Place', 'Impasse', 'Route', 'Chemin', 'Boulevard', 
            'Allées', 'Esplanade', 'Port', 'Promenade', 'Quai', 'Passage', 'Cheminement', 
            'Voie', 'Descente', 'Square', 'Contre-Allée', 'Cours', 'Parvis','Périphérique']

mapping = {'route' :'Route',
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

def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(file):
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])

    return street_types


# In[40]:


# We pass the fuctions to see the details of not expected names
st_types = audit(Tls_0)
pprint.pprint(dict(st_types))


# After investigation we find that some street names are correct and other need to be change
# We will update our expected value list with the names that are correct, the mapping list too if needed and we will create a change list for the changes we want to implement.
# We will procede like this: 
# 1. 'Grande' is correct we correct our expected values list
# 2. '6 Impasse Leonce Couture' should become ' Impasse Leonce Couture, 6' 
# 3. 'voie du T.O.E.C.' is correct we correct our expected values list and the mapping list
# 4. 'Lotissement': {'Lotissement Futuropolis - Impasse René Mouchotte'},should be Impasse René Mouchotte_Lotissement Futuropolis 
# 5. 'Frédéric': {'Frédéric Petit'}, should be Rue Frédéric Petit
# 6. 107  should be Cours Rosalind Franklin, 107
# 7. 9 should be Rue Reclusane, 9 
# 8. Angle adress refers to the places where videocam are placed. We will not correct that in our file
# 9. 'Sur': {'Sur facade du Théâtre face 1 place du Capitole','Sur parking face à la rue Porte Sardane'}, same than case 8
# 10. 'face': {'face 5 place du Capitole'} same than 8
# 11. to avoid error we pass angle, Sur and face as expected values  
# 
# We will use those informations later when preparing for CSV creation
# Lets continue now checking the file

# We investigate now a bit on the amenities - tag k="amenity" 

# In[48]:


# Functions to get the type of amenity 

amenity_type_re = re.compile(r'([\w.-]+)', re.IGNORECASE)
amenity_types = defaultdict(int)

def audit_amenity_type(amenity_types, amenity_name):
    m = amenity_type_re.search(amenity_name)
    if m:
        amenity_type = m.group()
        amenity_types[amenity_type] += 1

def print_sorted_dict(d, expression):
    keys = d.keys()
    keys = sorted(keys, key=lambda s: s.lower())
    for k in keys:
        v = d[k]
        print (expression % (k, v))

def is_amenity_name(elem):
    return (elem.tag == "tag") and (elem.attrib['k'] == "amenity")

def check_amenity(filename):
    for event, elem in ET.iterparse(filename):
        if is_amenity_name(elem):
            audit_amenity_type(amenity_types, amenity.attrib['v'])
    print(amenity_types, "%s: %d")
    return(amenity_types)

def check_amenity(filename):
    for event, elem in ET.iterparse(filename):
        if is_amenity_name(elem):
            audit_amenity_type(amenity_types, elem.attrib['v'])
    print(amenity_types, "%s: %d")
    return(amenity_types)


# In[49]:


# We pass the functions to Tls_0 and check the type of street
all_types = check_amenity(Tls_0)


# It seems correct and the names are in line with OSM standard
# We will see now the shops 

# In[50]:


# This show the street by type

Shops_type_re = re.compile(r'([\w.-]+)', re.IGNORECASE)

def is_Shops_name(elem):
    return (elem.attrib['k'] == "shop")
def audit_shops(filename):
    
    shops = {}
    for event, elem in ET.iterparse(filename):
        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_Shops_name(tag):
                    shop = tag.attrib['v']
                    if shop not in shops:
                        shops[shop] = 1
                    else:
                        shops[shop ] += 1
                
    return shops
                    

shops_types = audit_shops(Tls_0)
sorted_by_Shops = [(k, v) for (v, k) in sorted([(value, key) for (key, value) in shops_types.items()], reverse=True)]
pprint.pprint(sorted_by_Shops)


# The shop seams right but we want to investigate 'yes' and 'convenience;gas'. 
# We check that in OSM wiki and both are validated with some coments. We do not need to correct them
# 
# After those investigations we are ready to parse the file to CSV. We will do that in another document.
