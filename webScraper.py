#!/usr/bin/python3

# selenium imports
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# imports for file I/O
from collections import OrderedDict
import csv
import os
import os.path

import re #for regular expressions
import heapq # use priority queue when we move to multithreading model

# relative file paths of output files
g_urls_csv_file_path = '/MANGO/urls.csv'
g_items_csv_file_path = '/MANGO/items.csv'
g_domain = 'shop.mango.com/us/women'
g_blacklist = 'shop.mango.com/us/women/help/'

# global dictionary of all the urls to be visited
# key = url, value = AA containing status
g_new_urls = {
				'https://shop.mango.com/us/women/shirts-short-sleeve/flowy-textured-blouse_13090453.html' : {'priority' : 0 }
			 }
g_processing_urls = {}
g_processed_urls = {}
g_new_urls_heapq = [(0, 'https://shop.mango.com/us/women/shirts-short-sleeve/flowy-textured-blouse_13090453.html')]

# IMPORTANT! these columns are the final table columns, edit here when you increase or decrease the columns
g_urls_column = ['url', 'priority', 'status', 'outfitUrls', 'uniqueId']

# global dictionary of all fashion products like - top, bottom, dress, accessories etc.
# key = uniqueId (which is unique within a website), value = other metadata related to the item
g_items = {}

# IMPORTANT! these columns are the final table columns, edit here when you increase or decrease the columns
g_items_column = ['uniqueId', 'itemName', 'priceArray', 'color', 'description', 'imageUrls', 'url', 'outfitIds']

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
		print('++++++++++++++ pushing at ', str(aa['priority']), url)
		heapq.heappush(g_new_urls_heapq, (int(aa['priority']), url)) 

# moves a particular url from 'processing' pipeline to 'processed' pipeline when all the links are successfully extracted from it
# so we donot have to visit it again
# @url = url to update
# @uniqueId = uniqueid uniquiely identifies an item. Maintain uniqueID in the url dictionary to look it up in the items dictionary 
# @outfitUrls = array of urls of items from 'complete your outfit' section. These are needed to collect outfit unique IDs
def markUrlAsProcessed(url, uniqueId, outfitUrls=[]):
	print('++++++++++++++ markUrlAsProcessed', url)
	if url and url in g_processing_urls:	

		print('++++++++++++++ markUrlAsProcessed in processing', url)
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
		print('WARNING!! no new urls to process')
	
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

# specific per retailer MANGO
# @url to extract the features from
# @webdriver instance
def extractFeatures(url, driver):
	
	if isUrlProcessed(url):
		print ('extractFeatures() ', url, ' already processed. Returning immedietly.')
		return True

	try:
		result = False

		uniqueIdElem = driver.find_element_by_xpath('//*[@id="Form:SVFichaProducto:panelFicha"]/div[1]/div/div[1]/div[2]')
		#extract other features only if the unique id is found	
		if uniqueIdElem.text.find('REF') != -1:
			aa = {}
			uniqueId = uniqueIdElem.text

			#name of the product
			itemName = driver.find_element_by_xpath('//*[@id="Form:SVFichaProducto:panelFicha"]/div[1]/div/div[1]/div[1]/h1')
			aa['itemName'] = itemName.text

			#price and revised price
			price = driver.find_element_by_xpath('//*[@id="Form:SVFichaProducto:panelFicha"]/div[1]/div/div[2]/div')
			priceArray = re.split('\$|\n', price.text)
			aa['priceArray'] = set()
			#if the string is a number - integer or float
			for price in priceArray:
				if re.match("^\d+?\.\d+?$", price): 
					aa['priceArray'].add(float(price))

			#color
			hiddenDiv = driver.find_element_by_xpath('//*[@id="Form:SVFichaProducto:panelFicha"]/div[2]')
			color = hiddenDiv.get_attribute('textContent')
			color = color.strip('\t\n') #strip unwated characters
			colorArray = color.split(':')
			if len(colorArray) > 1:
				aa['color'] = colorArray[1].strip('\t\n')
			#TODO : get the alternate color also
			
			#description & material and washing instructions
			hiddenDiv = driver.find_element_by_xpath('//*[@id="Form:SVFichaProducto:panelFicha"]/div[7]')
			description = hiddenDiv.get_attribute('textContent')
			description = description.strip('\t\n') 
			descriptionArray = description.split('\n')
			strippedDescription = []
			for description in descriptionArray:
				text = description.strip('\t\n')
				if text != '':
					strippedDescription.append(text)

			aa['description'] = '|'.join(strippedDescription)

			#complete your outfit
			outfitUrls = []
			completeYourOutfit = driver.find_element_by_xpath('//*[@id="panelOufitsProducto"]')
			links = completeYourOutfit.find_elements_by_tag_name('a')
			for link in links:
				if link.is_displayed():
					href = sanitizeUrl(link.get_attribute('href'))
					outfitUrls.append(href)
					addUrlToDictionary(href, {'priority' : getPriority(url) + 1}) #non outfit urls have priority lower than outfits

			#extract image url
			imageDiv = driver.find_element_by_xpath('//*[@id="mainDivBody"]/div/div[5]/div[2]')
			images = imageDiv.find_elements_by_tag_name('img')

			aa['imageUrls'] = set()
			for image in images:
				aa['imageUrls'].add(image.get_attribute('src'))

			#set top level url 
			aa['url'] = url
			
			#print ("DEBUG %s " % str (aa))
			g_items[uniqueId] = aa

			markUrlAsProcessed(url, uniqueId, outfitUrls)	
			result = True

		else:
			print ("WARNING!! uniqueId text does not contain key word REF in url '", url, "'") 

		#find all the links in the page and add them to g_urls AFTER the outfit urls have been added so their priority is maintained
		allLinks = driver.find_elements_by_tag_name('a')
		for link in allLinks:
			href = sanitizeUrl(link.get_attribute('href'))
			if href and href.find(g_domain) != -1 and href.find(g_blacklist) == -1:
				addUrlToDictionary(href, {'priority' : getPriority(url) + 100}) #non outfit urls have priority lower than outfits

		return result 
	except NoSuchElementException:
		#mark as processed to prevent infinite loop
		markUrlAsProcessed(url, 'NO_ITEM_FOUND')	
		print ('WARNING!! uniqueId element not found in url ', url)
		return True

def appendOutfitId(itemId, outfitId):
	if itemId and outfitId and itemId in g_items:
		itemAA = g_items[itemId]
		if 'outfitIds' not in itemAA:
			itemAA['outfitIds'] = set()
		itemAA['outfitIds'].add(outfitId)
		g_items[itemId] = itemAA

def updateOutfitUniqueId(url, outfitUrl):
	print ('updateOutfitUniqueId()', url, outfitUrl)

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
	print ('loadUrlAndExtractData() ', getPriority(url),  url)
	# implicit wait will make the webdriver to poll DOM for x seconds when the element
	# is not available immedietly
	driver.implicitly_wait(10) # seconds
	driver.get(url)

	# wait till product catalog or the unique id is visible //*[@id="productCatalog"]
	try:
		extractFeatures(url, driver)
				
	except:
		print('WARNING!! caught exception for', url)

def convertAAtoArray(key, value):
	aa = {}
	if key.find('http') != -1:
		aa['url'] = key
	else:
		aa['uniqueId'] = key

	for k,v in value.items():
		aa[k] = v
	return aa

# if the file is not empty then open in append mode
def appendDictToCSV(csvFile, csvColumns, dictionary):
	try:
		with open(csvFile, 'a') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csvColumns)
			for key, value in dictionary.items():
				writer.writerow(convertAAtoArray(key, value))

	except IOError:
		print("I/O error({0}): {1}".format(errno, strerror))    


# if the file is empty then open in write mode and write header
def writeDictToCSV(csvFile, csvColumns, dictionary):
	try:
		with open(csvFile, 'w') as csvfile:
			writer = csv.DictWriter(csvfile, fieldnames=csvColumns)
			writer.writeheader()
			for key, value in dictionary.items():
				writer.writerow(convertAAtoArray(key, value))

	except IOError:
		print("I/O error({0}): {1}".format(errno, strerror))    


def readCSVToDict(csvFile):
	if os.path.exists(csvFile):
		try:
			with open(csvFile) as csvfile:
				reader = csv.DictReader(csvfile)
				for row in reader:
					#csv reader returns ordered dict, if the first item is a url then this is a g_urls dict
					key, value = row.popitem(False)
					if key == 'url':
						aa = dict(row)
						# add the urls in processed pipeline
						if aa['status'] == 'processed':
							g_processed_urls[value] = aa
						else:
							addUrlToDictionary(value, aa)
					elif key == 'uniqueId':
						g_items[value] = dict(row)

		except IOError:
			print("I/O error({0}): {1}".format(errno, strerror))
		return

def saveSessionOutput():

	for url in g_processed_urls:
		outfitUrls = g_processed_urls[url]['outfitUrls']
		for newUrl in outfitUrls:
			updateOutfitUniqueId(url, newUrl)


	#save the output of this session in csv
	currentPath = os.getcwd()
	urlsCSVPath = currentPath + g_urls_csv_file_path 
	itemCSVPath = currentPath + g_items_csv_file_path
	writeDictToCSV(urlsCSVPath, g_urls_column, g_new_urls)
	appendDictToCSV(urlsCSVPath, g_urls_column, g_processing_urls)
	appendDictToCSV(urlsCSVPath, g_urls_column, g_processed_urls)
	writeDictToCSV(itemCSVPath, g_items_column, g_items)

def main():
	currentPath = os.getcwd()
	urlsCSVPath = currentPath + g_urls_csv_file_path 
	itemCSVPath = currentPath + g_items_csv_file_path
	
	#load the csv as dictionary	
	readCSVToDict(urlsCSVPath)
	readCSVToDict(itemCSVPath)

#	print ("DEBUG %s " % str (g_urls))
#	print ("DEBUG %s " % str (g_items))
	url = getNextUrlToProcess()

	while (url != ''):
		driver = webdriver.PhantomJS()
		loadUrlAndExtractData(url, driver)
		driver.quit()
		url = getNextUrlToProcess()


	#TODO: map unique IDs and clean up csv read/write

#main function
#python lets you use the same source file as a reusable module or standalone
#when python runs it as standalone, it sends __name__ with value "__main__"
if __name__ == "__main__":
	try:
		main()
	except:
		print ('Thrown exception in main. save session')
		saveSessionOutput()
