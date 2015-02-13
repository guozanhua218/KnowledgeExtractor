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
def processMetadata(record,f):


	#Replace common abbreviations.
	record.abstract = record.abstract.replace('i.e.', '');
	record.abstract = record.abstract.replace('e.g.', '');
	

	#Remove bracketed text
	p = re.compile('\\((.*?)\\)')
	record.abstract = re.sub(p,'',record.abstract)
	record.abstract = record.abstract.replace("-", " ");
	record.abstract = re.sub(r'[^ .a-zA-Z0-9]','', record.abstract)
	record.abstract = record.abstract.replace("  ", " ");
	record.abstract = record.abstract.replace(" . ", ". ");
	record.abstract = record.abstract.replace(".:", ". ");


	#Loop and store
	f.write(record.abstract + '\n')


#----------------------------------------------------------------------
def parseXMLdata(xmlfile,f):


	#Parse data
	count = 0;
	print '[INFO] Parsing'
	record = Record()
	for event, elem in etree.iterparse('data/' + xmlfile + '/' + xmlfile + '.xml', events=('start', 'end')):
		if event == 'start' and elem.tag == 'PubmedArticle':
			print 'Reading abtract: ' + str(count+1)
			badrecord = 0
		if event == 'start':
			if elem.tag == 'AbstractText':
				if (elem.text != None):
					record.abstract = elem.text.lower()
				else:
					badrecord = 1
		if elem.tag == 'PubmedArticle' and event == 'end':
			#Send article meta-data for processing
			if (badrecord == 0 and len(record.abstract) > 0):
				processMetadata(record,f)
				count = count + 1
			record.abstract = ""


#----------------------------------------------------------------------
if __name__ == '__main__':


	#Init	
	print '[INFO] XML Parser'

	#Parser
	file = open('data/' + sys.argv[1] + '/' + sys.argv[1] + '.abstracts','w')
	parseXMLdata(sys.argv[1],file)
	file.close()


