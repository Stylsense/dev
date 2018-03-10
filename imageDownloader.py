import click
import requests
import threading
import csv
import os
import os.path
import time
import logging
import traceback

g_delimiter = '|'
# IMPORTANT! these columns are the final table columns, edit here when you increase or decrease the columns
g_items_column = ['uniqueId', 'itemName', 'category', 'priceArray', 'color', 'description', 'imageUrls', 'url', 'outfitIds', 'imageDownloadStatus']
# global dictionary of all fashion products like - top, bottom, dress, accessories etc.
# key = uniqueId (which is unique within a website), value = other metadata related to the item
g_items = {}
g_items_csv_file_path = '/MANGO/items_image_status.csv'
g_images_output_path = '/MANGOIMAGES/'

g_logger_file_path = '/session-logs/' #prefix with date
loggerFilePath = os.getcwd()+ g_logger_file_path + time.strftime('%m-%d-%y') + '.log'  

#create a logger for webscraper
g_logger = logging.getLogger('imageDownloader')
g_logger.setLevel(logging.DEBUG)

#create file handler which logs even debug messages
fh = logging.FileHandler(loggerFilePath)
fh.setLevel(logging.DEBUG)

#create console handler with same log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

#create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
# The below code is used for each chunk of file handled
# by each thread for downloading the content from specified 
# location to storage
def Handler(start, end, url, filename):
    # specify the starting and ending of the file
    headers = {'Range': 'bytes=%d-%d' % (start, end)}

    # request the specified part and get into variable    
    r = requests.get(url, headers=headers, stream=True)

    # open the file and write the content of the html page 
    # into file.
    with open(filename, "r+b") as fp:
        fp.seek(start)
        var = fp.tell()
        fp.write(r.content)

#def download_file(ctx,url_of_file,name,number_of_threads):
def download_file(url_of_file,path,name,number_of_threads):
    r = requests.head(url_of_file)
    if name:
        file_name = path + name
    else:
        file_name = path + url_of_file.split('/')[-1]
    try:
        file_size = int(r.headers['content-length'])
    except:
        g_logger.error("Invalid URL")
        return False

    try:
        g_logger.debug('+++++++++', file_size)
        part = int(file_size) / number_of_threads
        fp = open(file_name, "wb")
        fp.write(b"\0" * file_size)
        fp.close()

        for i in range(number_of_threads):
            start = int(part * i)
            end = int(start + part)

            # create a Thread with start and end locations
            t = threading.Thread(target=Handler,
            kwargs={'start': start, 'end': end, 'url': url_of_file, 'filename': file_name})
            t.setDaemon(True)
            t.start()

            main_thread = threading.current_thread()
            for t in threading.enumerate():
                if t is main_thread:
                    continue
                t.join()
            print('%s downloaded' % file_name)

        return True
    except:
        g_logger.exception('FAILED to download' % file_name)
        return False


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

        if key == 'uniqueId':
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

#converts python internal data structure to appropriate format
def convertAAtoRow(key, value):
	aa = {}
	if key.find('http') != -1:
            aa['url'] = key
	elif key.find('REF') != -1:
            aa['uniqueId'] = key
	else:
            aa['category'] = key

	for k,v in value.items():
            #convert list to string because csv doesn't support lists
            if isinstance(v, list) or isinstance(v, set):
                v = g_delimiter.join(v)
                    
            aa[k] = v
	return aa

#CSV writer flattens all data to string, this function will change it back as per the key type
#TODO: move to a better data marshalling scheme
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


# sanitize url, throw away query params
def sanitizeUrl(url):
    if url:
        queryParam = url.rfind('?')
        if queryParam != -1:
            url =  url[:queryParam]
    return url 

def downloadAllImages():
    directory = os.getcwd() + g_images_output_path
    for item in g_items:
        itemDirectory = directory + item + "/"
        if not os.path.exists(itemDirectory):
            os.makedirs(itemDirectory)
        g_logger.debug('++++++', itemDirectory)
        for imageurl in g_items[item]['imageUrls']:
            url = sanitizeUrl(imageurl)
            if url:
                if not download_file(url,itemDirectory,'',5):
                    g_items[item]['imageDownloadStatus'] = 'failed'

#main function
#python lets you use the same source file as a reusable module or standalone
#when python runs it as standalone, it sends __name__ with value "__main__"
if __name__ == "__main__":
    try:
        currentPath = os.getcwd()
        itemCSVPath = currentPath + g_items_csv_file_path
        print('DEBUG', itemCSVPath)
        #load the csv as dictionary	
        readCSVToDict(itemCSVPath)
        downloadAllImages()
        g_logger.debug('Program finished, items in dictionary %d', len(g_items))
        writeDictToCSV(itemCSVPath, g_items_column, g_items)

    except:
        #keyboard interrupt
        g_logger.debug('Program finished, with exception %d', len(g_items))
        traceback.print_exc()
        if g_items:
            writeDictToCSV(itemCSVPath, g_items_column, g_items)
        
#@click.command(help="It downloads the specified file with specified name")
#@click.option('--number_of_threads',default=4, help="No of Threads")
#@click.option('--name',type=click.Path(),help="Name of the file with extension")
#@click.argument('url_of_file',type=click.Path())
#@click.pass_context
#main function
#if __name__ == '__main__':
#    download_file(obj={})

