import numpy
import scipy
import sys
import struct
import array
import xml.etree.ElementTree as etree
import re

class Record:
	lastname = []
	initials = []
	year = ''
	month = ''
	ID = ''
	abstract = ''
	def show(self):
		print self.lastname
		print self.initials
		print self.year
		print self.month
		print self.ID
		print self.abstract



#----------------------------------------------------------------------
def processMetadata(record,f,stopwords):


	#Replace common abbreviations.
	record.abstract = record.abstract.replace('i.e.', '');
	record.abstract = record.abstract.replace('e.g.', '');
	record.abstract = record.abstract.replace("-", " ");

	#Remove bracketed text
	p = re.compile('\\((.*?)\\)')
	record.abstract = re.sub(p,'',record.abstract)
	record.abstract = re.sub(r'[^ .a-zA-Z0-9]','', record.abstract)
	record.abstract = record.abstract.replace("  ", " ");
	record.abstract = record.abstract.replace(" . ", ". ");
	orig = record.abstract

	#Loop and store
	sentences = record.abstract.split('.')
	for sentence in sentences:
		words = sentence.split(' ')
		previousword1 = ''
		previousword2 = ''
		previousword3 = ''
		previousword4 = ''
		for word in words:
			word = word.encode('utf-8')
			if word in stopwords or len(word) == 0:
				previousword1 = ''
				previousword2 = ''
				previousword3 = ''
				previousword4 = ''
			else:
				f.write(record.ID + "," + previousword4 + "," + previousword3 + "," + previousword2 + "," + previousword1 + "," + word + "\n")
				previousword4 = previousword3
				previousword3 = previousword2
				previousword2 = previousword1
				previousword1 = word



#----------------------------------------------------------------------
def parseXMLdata(xmlfile,f,stopwords):


	#Parse data
	count = 0;
	print '[INFO] Parsing'
	record = Record()
	for event, elem in etree.iterparse('data/' + xmlfile + '/' + xmlfile + '.xml', events=('start', 'end')):
		if event == 'start' and elem.tag == 'PubmedArticle':
			print 'Reading abtract: ' + str(count+1)
			badrecord = 0
		if event == 'start':
			if elem.tag == 'LastName':
				if (elem.text != None):
					record.lastname.append(elem.text.lower())
				else:
					badrecord = 0
			if elem.tag == 'Initials':
				if (elem.text != None):
					record.initials.append(elem.text.lower())
				else:
					badrecord = 0
			if elem.tag == 'Month':
				if (elem.text != None):
					record.month = elem.text
				else:
					badrecord = 0
			if elem.tag == 'Year':
				if (elem.text != None):
					record.year = elem.text
				else:
					badrecord = 1
			if elem.tag == 'PMID':
				if (elem.text != None):
					record.ID = elem.text
				else:
					badrecord = 1
			if elem.tag == 'AbstractText':
				if (elem.text != None):
					record.abstract = elem.text.lower()
				else:
					badrecord = 1
		if elem.tag == 'PubmedArticle' and event == 'end':
			#Send article meta-data for processing
			if (badrecord == 0 and len(record.abstract) > 0):
				processMetadata(record,f,stopwords)
				count = count + 1
			record.lastname = []
			record.initials = []
			record.abstract = ""


#----------------------------------------------------------------------
def loadStopwords():

	stopwords = []
	with open('data/stopwords','r') as f:
		for lines in f:
			lines = lines.rstrip()
			stopwords.append(lines)
			print lines

	return stopwords


#----------------------------------------------------------------------
if __name__ == '__main__':


	#Init	
	print '[INFO] XML Parser'

	#Load Stopwords
	stopwords = loadStopwords()

	#Parser
	file = open('data/' + sys.argv[1] + '/' + sys.argv[1] + '.tokens','w')
	parseXMLdata(sys.argv[1],file,stopwords)
	file.close()


