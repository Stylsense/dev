#!/usr/bin/python3

# imports for file I/O
from collections import OrderedDict
import csv
import os
import os.path
import re #for regular expressions
import time
import logging
import platform
import traceback

g_delimiter = '|'
# IMPORTANT! these columns are the final table columns, edit here when you increase or decrease the columns
g_items_column = ['uniqueId', 'itemName', 'category', 'priceArray', 'color', 'description', 'imageUrls', 'url', 'outfitIds']
# global dictionary of all fashion products like - top, bottom, dress, accessories etc.
# key = uniqueId (which is unique within a website), value = other metadata related to the item
g_items = {}
g_categories = {}
g_items_csv_file_path = '/items_in.csv'
g_logger = logging.getLogger('datapostprocess')
g_logger.setLevel(logging.DEBUG)
#create console handler with same log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
g_logger.addHandler(ch)

#feature column
#uniqueId    itemName    category    priceArray  color   description 

#g_feature_column = ['composition_1','composition_2','composition_3','composition_4','composition_5','composition_percent_1','composition_percent_2','composition_percent_3','composition_percent_4','composition_percent_5','fabric','sleeve','neckline','lapels','cuffs','detail','design','texture','finish','clasp','closure','button','strap','lining','effect','seams','applique', 'collar'] 
g_feature_column = ['fabric','sleeve','neckline','lapels','cuffs','detail','design','texture','finish','clasp','closure','button','strap','effect','seams','collar'] 
#imageUrls   url outfitIds
g_categories = set([
'shirts-tops',
'pants',
'shirts-blouses',
'cardigans-and-sweaters-sweaters',
'coats-trenchs',
'jackets-biker-jackets',
'jackets-jackets',
'pants-straight',
'skirts-midi',
't-shirts-and-tops-short-sleeve',
'jackets',
'jeans-skinny',
'jeans-straight',
'jeans-wide-leg',
'pants-loose',
'shirts-shirts',
'skirts-short',
't-shirts-and-tops-long-sleeve',
't-shirts-and-tops',
'cardigans-and-sweaters-cardigans',
'cardigans-and-sweaters',
'coats-coats',
'coats-parkas',
'coats-puffer---quilted',
'jackets-blazers',
'jeans',
'shirts-long-sleeve',
'sweatshirts',
'jeans-relaxed',
'pants-leggings',
't-shirts-and-tops-tank-tops',
'jackets-vests',
'pants-skinny',
'shorts',
'jackets-denim',
'shirts-short-sleeve',
'pants-wide-leg',
'skirts-long',
'shirts',
'coats',
'skirts'])

def splitDescription(descriptionBlob, pattern):
    aa = {}
    if descriptionBlob and pattern:
        descriptionArray = re.split('\|', descriptionBlob)
        for item in descriptionArray:
            #pattern = 'fabric|sleeve|neckline|lapels|cuffs|detail|design|texture|finish|clasp|closure|button|strap|effect|seams|collar' 
            match = re.findall(pattern,item,re.IGNORECASE)
            if match:
                aa[match[0].lower()] = item

    return aa

def readCSVToDict(csvFile):
    if os.path.exists(csvFile):
        try:
            with open(csvFile) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    convertRowToAA(row)

        except IOError:
            g_logger.error("I/O error({0}): {1}".format(errno, strerror))

def convertRowToAA(row):
    if row:
        # ecah row is an ordered dictionary
        # row.popitem(False) will give out the items in FIFO manner, 
        # which means first item to be popped will be either 'url' or 'uniqueId'
        key, value = row.popitem(False)
        aa = sanitizeCSVRow(row) #row now contains rest of the items except the url/uniqueid
        pattern = g_delimiter.join(g_feature_column) 

        if aa['category'] in g_categories and key == 'uniqueId':
            descriptionAA = splitDescription(aa['description'], pattern)

            if descriptionAA:
                aa.update(descriptionAA)
            #print ('++++++++++', aa)
            uniqueId = value
            g_items[uniqueId] = aa 

#CSV writer flattens all data to string, this function will change it back as per the key type
#TODO: move to a better data marshalling scheme
def sanitizeCSVRow(row):
    aa = {} 
    while row:
        key, value = row.popitem(False)
        if key == 'imageUrls' or key == 'priceArray' or key == 'outfitIds' or key == 'outfitUrls':
            aa[key] = set(value.split(g_delimiter))
        else:
            aa[key] = value
    return aa

def convertAAtoRow(key, value):
    aa = {}
    if key.find('REF') != -1:
        aa['uniqueId'] = key

    for k,v in value.items():
        #convert list to string because csv doesn't support lists
        if isinstance(v, list) or isinstance(v, set):
            v = g_delimiter.join(v)
            
        aa[k] = v
    return aa


# if the file is empty then open in write mode and write header
def writeDictToCSV(csvFile, csvColumns, dictionary):
    try:
        with open(csvFile, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csvColumns)
            writer.writeheader()
            for key, value in dictionary.items():
                writer.writerow(convertAAtoRow(key, value))

    except IOError:
        g_logger.error("I/O error({0}): {1}".format(errno, strerror))    

def main():
    currentPath = os.getcwd()
    itemCSVPath = currentPath + g_items_csv_file_path
    print ("DEBUG", itemCSVPath)
    #load the csv as dictionary	
    readCSVToDict(itemCSVPath)

#main function
#python lets you use the same source file as a reusable module or standalone
#when python runs it as standalone, it sends __name__ with value "__main__"
if __name__ == "__main__":
    try:
        main()
        g_logger.debug('Program finished, items in dictionary %d', len(g_items))
        g_items_column = g_items_column + g_feature_column
        print ('++++++++++++++++', g_items_column)
        writeDictToCSV(os.getcwd() + '/items_out.csv', g_items_column, g_items)

    except:
        #keyboard interrupt
        g_logger.debug('Program finished, with exception %d', len(g_items))
        traceback.print_exc()
        
