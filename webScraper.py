#!/usr/bin/python3

# selenium imports
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException 
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions

# imports for file I/O
from collections import OrderedDict
import csv
import os
import os.path

import re #for regular expressions
import heapq # use priority queue when we move to multithreading model
import time
import logging
import uuid


# relative file paths of output files
g_urls_csv_file_path = '/MANGO/urls.csv'
g_items_csv_file_path = '/MANGO/items.csv'
g_item_count_csv_file_path = '/MANGO/itemsCount.csv'
g_domain = 'shop.mango.com/us/women'
g_blacklist = ['shop.mango.com/us/women/help/', 'shop.mango.com/us/men']
g_delimiter = '|'
g_item_count_per_category = {}
g_logger_file_path = '/session-logs/' #prefix with date
loggerFilePath = os.getcwd()+ g_logger_file_path + time.strftime('%m-%d-%y') + '.log'  

#create a logger for webscraper
g_logger = logging.getLogger('webscraper')
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
g_logger.addHandler(fh)
g_logger.addHandler(ch)

# global dictionary of all the urls to be visited
# key = url, value = AA containing status
g_new_urls = {
				'https://shop.mango.com/us/women/dresses-midi/double-breasted-dress_21033666.html' : {'priority' : 0} 
			 }
g_processing_urls = {}
g_processed_urls = {}
g_new_urls_heapq = [(0, 'https://shop.mango.com/us/women/dresses-midi/double-breasted-dress_21033666.html')]

# IMPORTANT! these columns are the final table columns, edit here when you increase or decrease the columns
g_urls_column = ['url', 'priority', 'status', 'outfitUrls', 'uniqueId']

# global dictionary of all fashion products like - top, bottom, dress, accessories etc.
# key = uniqueId (which is unique within a website), value = other metadata related to the item
g_items = {}

# IMPORTANT! these columns are the final table columns, edit here when you increase or decrease the columns
g_items_column = ['uniqueId', 'itemName', 'category', 'priceArray', 'color', 'description', 'imageUrls', 'url', 'outfitIds']




# sanitize url, throw away query params
def sanitizeUrl(url):
	if url:
		queryParam = url.rfind('?')
		if queryParam != -1:
			url =  url[:queryParam]
	return url 

# sample row in the table
#			{
#				'uniqueId' : '',
#				'itemName' : '',
#				'priceArray' : 0.0,
#				'color': '',
#				'description' : '',
#				'outfitIds' : ['id1', 'id2']   						
#			}

# this function will add a new url to new urls dictionary ONLY if it non empty and not in processing or processed queue 
# it will also add the url to a min heapq based on its priority
# @url 
def addUrlToDictionary(url, aa):
	if url and url not in g_new_urls and url not in g_processing_urls and url not in g_processed_urls:
		g_new_urls[url] = aa
		#g_logger.debug('++++++++++++++ pushing at ', str(aa['priority']), url)
		heapq.heappush(g_new_urls_heapq, (int(aa['priority']), url)) 

# moves a particular url from 'processing' pipeline to 'processed' pipeline when all the links are successfully extracted from it
# so we donot have to visit it again
# @url = url to update
# @uniqueId = uniqueid uniquiely identifies an item. Maintain uniqueID in the url dictionary to look it up in the items dictionary 
# @outfitUrls = set of urls of items from 'complete your outfit' section. These are needed to collect outfit unique IDs
def markUrlAsProcessed(url, uniqueId, outfitUrls):
	if url and url in g_processing_urls:	
		# move the url to 'processed' pipeline
		aa = g_processing_urls[url]
		g_processed_urls[url] = aa 
		g_processed_urls[url]['uniqueId'] = uniqueId
		g_processed_urls[url]['status'] = 'processed' #maintain the status for CSV 
		if outfitUrls:
			g_processed_urls[url]['outfitUrls'] = outfitUrls
		# remove it from the processing pipeline
		del g_processing_urls[url]

	#by this point, url has already been moved to processed pipeline


# get the next url to process from the min heapq
# removes a url from the 'new' pipeline and moves it to 'processing' pipeline 
def getNextUrlToProcess():
	url = ''

	if g_new_urls_heapq:
		tup = heapq.heappop(g_new_urls_heapq)
		url = tup[1]
		
		if url in g_new_urls:
			aa = g_new_urls[url]
			aa['status'] = 'processing'
			g_processing_urls[url] = aa
			del g_new_urls[url]
	else:
		g_logger.warning('no new urls to process')
	
	return url

# return the priority of the url
def getPriority(url):
	aa = {}
	if url in g_new_urls:
		aa = g_new_urls[url]
	elif url in g_processing_urls:
		aa = g_processing_urls[url]
	elif url in g_processed_urls:
		aa = g_processed_urls[url]

	return int(aa['priority'])

# check if this url is already processed 
# @url
# returns True/False
def isUrlProcessed(url):
	return url and url in g_processed_urls

#typically mango urls are https://shop.mango.com/us/women/coats_c67886633 OR https://shop.mango.com/us/women/coats-coats/oversize-wool-coat_11047664.html
#string after women represents category, more reliable than the one in the webpage
def extractCetegoryFromUrl(url):
	result = ''
	if url:
		values = url.split('/')
		if len(values) > 5:
			result = values[5]
	return result

#check if the domain matches any of the blacklisted domains
def isBlacklistedDomain(url):
	for domain in g_blacklist:
		if url.find(domain) != -1:
			return True
	return False

# specific per retailer MANGO
# @url to extract the features from
# @webdriver instance
def extractFeatures(url, driver):
	
	if isUrlProcessed(url):
		g_logger.debug('extractFeatures() %s already processed. Returning immedietly.', url)
		return True

	try:
		result = False
		uniqueIdElem = driver.find_element_by_xpath("//*[@id='Form:SVFichaProducto:panelFicha']/div[1]/div/div[1]/div[2]")
		#extract other features only if the unique id is found	
		if uniqueIdElem.text.find('REF') != -1:
			aa = {}
			uniqueId = uniqueIdElem.text

			#name of the product
			itemName = driver.find_element_by_xpath("//*[@id='Form:SVFichaProducto:panelFicha']/div[1]/div/div[1]/div[1]/h1")
			aa['itemName'] = itemName.text

			#category of the product
			aa['category'] = extractCetegoryFromUrl(url)

			#price and revised price
			price = driver.find_element_by_xpath("//*[@id='Form:SVFichaProducto:panelFicha']/div[1]/div/div[2]/div")
			priceArray = re.split('\$|\n', price.text)
			aa['priceArray'] = set()
			#if the string is a number - integer or float
			for price in priceArray:
				if re.match("^\d+?\.\d+?$", price) and price: 
					aa['priceArray'].add(str(price))

			#color
			hiddenDiv = driver.find_element_by_xpath("//*[@id='Form:SVFichaProducto:panelFicha']/div[2]")
			color = hiddenDiv.get_attribute('textContent')
			color = color.strip('\t\n') #strip unwated characters
			colorArray = color.split(':')
			if len(colorArray) > 1:
				aa['color'] = colorArray[1].strip('\t\n')
			#TODO : get the alternate color also
			
			#description & material and washing instructions
			hiddenDiv = driver.find_element_by_xpath("//*[@id='Form:SVFichaProducto:panelFicha']/div[7]")
			description = hiddenDiv.get_attribute('textContent')
			description = description.strip('\t\n') 
			descriptionArray = description.split('\n')
			strippedDescription = []
			for description in descriptionArray:
				text = description.strip('\t\n')
				if text != '':
					strippedDescription.append(text)

			aa['description'] = g_delimiter.join(strippedDescription)

			#complete your outfit
			outfitUrls = set()
			completeYourOutfit = driver.find_element_by_xpath("//*[@id='panelOufitsProducto']")
			links = completeYourOutfit.find_elements_by_tag_name('a')
			for link in links:
				if link.is_displayed():
					href = sanitizeUrl(link.get_attribute('href'))
					if href and href.find(g_domain) != -1 and not isBlacklistedDomain(href):
						outfitUrls.add(href)
						addUrlToDictionary(href, {'priority' : getPriority(url) + 1}) #non outfit urls have priority lower than outfits

			#extract image url
			imageDiv = driver.find_element_by_xpath("//*[@id='mainDivBody']/div/div[5]/div[2]")
			images = imageDiv.find_elements_by_tag_name('img')

			aa['imageUrls'] = set()
			for image in images:
				attr = image.get_attribute('src')
				if attr:
					aa['imageUrls'].add(attr)

			#set top level url 
			aa['url'] = url
			
			#g_logger.debug("%s " % str (aa))
			g_items[uniqueId] = aa

			markUrlAsProcessed(url, uniqueId, outfitUrls)	
			result = True

		else:
			g_logger.warning('uniqueId text does not contain key word REF in url %s', url) 


		#find all the links in the page and add them to g_urls AFTER the outfit urls have been added so their priority is maintained
		allLinks = driver.find_elements_by_tag_name('a')
		for link in allLinks:
			href = sanitizeUrl(link.get_attribute('href'))
			if href and href.find(g_domain) != -1 and not isBlacklistedDomain(href):
				addUrlToDictionary(href, {'priority' : getPriority(url) + 100}) #non outfit urls have priority lower than outfits

		return result 
	except NoSuchElementException:

		filename = 'FAILEDPAGES/' + str(uuid.uuid4()) + '.png'
		driver.save_screenshot(filename)
		#mark as processed to prevent infinite loop
		#check if this is a catalog page, if so count the number of products
		button = driver.find_element_by_xpath("//*[@id='navColumns4']")
		button.click()

		wait_for(link_has_gone_stale, button)

		productCatalog = driver.find_element_by_xpath("//*[@id='productCatalog']")
		products = set(productCatalog.find_elements_by_tag_name('a'))
		loading = True
		while products and loading: 
			#products get dynamically loaded, so scroll to the bottom of the page#
			driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")		

			#give couple of seconds to load more items
			time.sleep(2)

			newProducts = set(productCatalog.find_elements_by_tag_name('a'))
			if newProducts and newProducts.difference(products):
				products = products.union(newProducts)
			else: 
				loading = False

		category = extractCetegoryFromUrl(url)
		g_logger.debug('found %d items in category %s', len(products), category)
		
		g_item_count_per_category[category] = {'count' : len(products)}
		
		#DEBUG for product in products:
		#DEBUG	print ('+++++++++++', product.get_attribute('title'))

		markUrlAsProcessed(url, 'NO_ITEM_FOUND', set())	
		g_logger.warning('uniqueId element not found in url %s', url)
		return True

def wait_for(condition_function, condition_function_args):
	start_time = time.time()
	while time.time() < start_time + 3:
		if condition_function(condition_function_args):
			return True
		else:
			time.sleep(0.1)
	raise Exception(
		'Timeout waiting for {}'.format(condition_function.__name__, condition_function_args.tag_name)
	)

def link_has_gone_stale(seleniumWebelement):
	try:
		# poll the link with an arbitrary call
		seleniumWebelement.find_elements_by_id('doesnt-matter') 
		return False
	except StaleElementReferenceException:
		g_logger.exception('stale element exception') 
		return True

def appendOutfitId(itemId, outfitId):
	if itemId and outfitId and itemId in g_items:
		itemAA = g_items[itemId]
		if 'outfitIds' not in itemAA:
			itemAA['outfitIds'] = set()
		itemAA['outfitIds'].add(outfitId)
		g_items[itemId] = itemAA

def updateOutfitUniqueId(url, outfitUrl):
	if url in g_processed_urls and outfitUrl in g_processed_urls:
		urlAA = g_processed_urls[url]
		outfitUrlAA = g_processed_urls[outfitUrl]
		if 'uniqueId' in urlAA and 'uniqueId' in outfitUrlAA:
			itemId = urlAA['uniqueId']
			outfitId = outfitUrlAA['uniqueId']
			appendOutfitId(itemId, outfitId)
			appendOutfitId(outfitId, itemId)

#load new url
def loadUrlAndExtractData(url, driver):
	g_logger.debug('loadUrlAndExtractData() %d, %s', getPriority(url),  url)
	# implicit wait will make the webdriver to poll DOM for x seconds when the element
	# is not available immedietly
	driver.implicitly_wait(7) # seconds
	driver.get(url)

	# wait till product catalog or the unique id is visible //*[@id="productCatalog"]
	try:
		extractFeatures(url, driver)
				
	except:
		g_logger.exception('caught exception for %s', url)


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
def sanitizeCSVRow(row):
	aa = {} 
	while row:
		key, value = row.popitem(False)
		if key == 'imageUrls' or key == 'priceArray' or key == 'outfitIds' or key == 'outfitUrls':
			aa[key] = set(value.split(g_delimiter))
		else:
			aa[key] = value

	return aa

#converts a csv row which is an ordered dict to python internal data structure
def convertRowToAA(row):
	if row:
		# ecah row is an ordered dictionary
		# row.popitem(False) will give out the items in FIFO manner, 
		# which means first item to be popped will be either 'url' or 'uniqueId'
		key, value = row.popitem(False)
		aa = sanitizeCSVRow(row) #row now contains rest of the items except the url/uniqueid
		if key == 'url':
			url = value
			# add the urls in processed pipeline
			if aa['status'] == 'processed':
				g_processed_urls[url] = aa
			else:
				addUrlToDictionary(url, aa)
		elif key == 'uniqueId':
			uniqueId = value
			g_items[uniqueId] = aa 
		elif key == 'category':
			category = value
			g_item_count_per_category[category] = aa 


# if the file is not empty then open in append mode
def appendDictToCSV(csvFile, csvColumns, dictionary):
	try:
		with open(csvFile, 'a') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csvColumns)
			for key, value in dictionary.items():
				writer.writerow(convertAAtoRow(key, value))

	except IOError:
		g_logger.error("I/O error({0}): {1}".format(errno, strerror))    


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


def readCSVToDict(csvFile):
	if os.path.exists(csvFile):
		try:
			with open(csvFile) as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					convertRowToAA(row)

		except IOError:
			g_logger.error("I/O error({0}): {1}".format(errno, strerror))
		return

def saveSessionOutput():
	g_logger.debug('Save session')
	for url in g_processed_urls:
		urlAA = g_processed_urls[url]
		if 'outfitUrls' in urlAA: 
			outfitUrls = urlAA['outfitUrls']
			for newUrl in outfitUrls:
				updateOutfitUniqueId(url, newUrl)


	#save the output of this session in csv
	currentPath = os.getcwd()
	urlsCSVPath = currentPath + g_urls_csv_file_path 
	itemCSVPath = currentPath + g_items_csv_file_path
	itemCountCSVPath = currentPath + g_item_count_csv_file_path
	writeDictToCSV(urlsCSVPath, g_urls_column, g_new_urls)
	appendDictToCSV(urlsCSVPath, g_urls_column, g_processing_urls)
	appendDictToCSV(urlsCSVPath, g_urls_column, g_processed_urls)
	writeDictToCSV(itemCSVPath, g_items_column, g_items)
	writeDictToCSV(itemCountCSVPath, ['category', 'count'], g_item_count_per_category)

def main():
	currentPath = os.getcwd()
	urlsCSVPath = currentPath + g_urls_csv_file_path 
	itemCSVPath = currentPath + g_items_csv_file_path
	itemCountCSVPath = currentPath + g_item_count_csv_file_path


	#load the csv as dictionary	
	readCSVToDict(urlsCSVPath)
	readCSVToDict(itemCSVPath)
	readCSVToDict(itemCountCSVPath)

	#print ("DEBUG %s " % str (g_new_urls))
	#print ("DEBUG %s " % str (g_items))
	url = getNextUrlToProcess()
	
	count = 1
	while (url != ''):
		driver = webdriver.PhantomJS()
		loadUrlAndExtractData(url, driver)
		driver.quit()
		url = getNextUrlToProcess()
		count += 1
		if count > 20:
			count = 0
			saveSessionOutput()

	#TODO: map unique IDs and clean up csv read/write

#main function
#python lets you use the same source file as a reusable module or standalone
#when python runs it as standalone, it sends __name__ with value "__main__"
if __name__ == "__main__":
	try:
		main()
		g_logger.debug('Program finished without any exception. Save session')
		saveSessionOutput()
	except:
		g_logger.exception('Thrown exception in main. Save session')
		saveSessionOutput()
