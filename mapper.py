from pyspark import SparkContext
import sys
import os
import math

spark_content = SparkContext("local","PiyushZode")

main_file = ""
cnt = 0

stop_words = ['a','able','about','across','after','all','almost','also','am','among','an','and','any','are','as','at','be','because','been','but','by','can','cannot','could','dear','did','do','does','either','else','ever','every','for','from','get','got','had','has','have','he','her','hers','him','his','how','however','i','if','in','into','is','it','its','just','least','let','like','likely','may','me','might','most','must','my','neither','no','nor','not','of','off','often','on','only','or','other','our','own','rather','said','say','says','she','should','since','so','some','than','that','the','their','them','then','there','these','they','this','tis','to','too','twas','us','wants','was','we','were','what','when','where','which','while','who','whom','why','will','with','would','yet','you','your']


new_li = []
new_items = {}

def fileName(data):
	string = data.toDebugString

def mapper_reducer1(filename,text):
	#words = filename
	#global main_file
	#global cnt
	#cnt = cnt+1
	#if(filename!=''):
	#	main_file = filename
	#	value = main_file
	#	return (value)
	#else:
	#	return ''
	
	return (str(mystr) +' - '+str(filename) for mystr in text if mystr.lower() not in stop_words)


def mapper_reducer12(filename,entire_text):
	myli=[]
	removed_n = entire_text.replace("\n","")
	text_li=removed_n.split(" ")
	global new_items
	new_items[filename] = len(text_li)

	new_len = 0
	for word in text_li:
		if word.lower() not in stop_words:
			new_len = new_len+1

	for word in text_li:
		if word.lower() not in stop_words:
			tmp = word+" - "+filename+" & "+str(new_len)
			myli.append(tmp)
	print "Length is %s" % new_items
	return myli




def mapper_reducer2(word_filename,count):
	li = []
	items = {}
	print "value of word_filename is %s" % word_filename
	temp = 	word_filename.split(' - ')
	word = temp[0]
	filename = temp[1]
	key = filename
	value = str(word)+' - '+str(count)
	items['word']=word
	items['filename']=filename
	items['count']=count

	#li.append(items)
	#for mystr in word_filename:
		#print "value of mystr is %s" % mystr
		#temp = 	mystr.split(' - ')
		#word = mystr[0]
		#filename = temp[1]
		#key = filename
		#value = word+' - '+count
		#li.append(key,value)
	#print "Value of li is: %s" % li
	#return (str(word)+','+str(filename)+','+str(count))
	mystring = str(filename)+','+str(word)+' - '+str(count)
	print "Value of li is: %s" % mystring
	li.append(mystring)
	print "li : %s" % li
	

	#new_li.append(new_items)
	return li
	#return()


def mapper_reducer21(word_filename,count):
	li = []
	items = {}
	

	print "value of word_filename is %s" % word_filename
	temp = 	word_filename.split(' - ')
	word = temp[0]
	filename = temp[1]
	key = filename
	value = str(word)+' - '+str(count)
	items['word']=word
	items['filename']=filename
	items['count']=count
	#li.append(items)
	#for mystr in word_filename:
		#print "value of mystr is %s" % mystr
		#temp = 	mystr.split(' - ')
		#word = mystr[0]
		#filename = temp[1]
		#key = filename
		#value = word+' - '+count
		#li.append(key,value)
	#print "Value of li is: %s" % li
	#return (str(word)+','+str(filename)+','+str(count))
	#('file:/home/piyush/Downloads/spark_assignment/spark-2.0.0-bin-hadoop2.7/input/08.txt,international - 1', 1)
	mystring = str(filename)+","+str(word)+" - "+str(count)
	print "Value of li is: %s" % mystring
	li.append(mystring)
	print "Li is:" % li


	return li

def flatten_test(pair):
	f, text = pair
	print "filename %s" % f
	print "b %s" % text
	return [line.split(",") + [f] for line in text.splitlines()]
#	return filename
	#print "filename %s" % filename
	

#text_file = spark_content.textFile("input")
#text_file = spark_content.wholeTextFiles("input")
#count = spark_content.wholeTextFiles("input").flatMap(lambda (filename,text): mapper_reducer12(filename,text.encode('utf-8').split())) #\
             #.map(lambda a: (a, 1)) \
             #.reduceByKey(lambda a, b: a + b) \
	     #.flatMap(lambda(a, b):mapper_reducer2(a,b)) \
 	     #.map(lambda a: (a.split(',')[0],a.split(',')[1]))

#('University - file:/home/piyush/Downloads/spark_assignment/spark-2.0.0-bin-hadoop2.7/input/02.txt & 394', 1)

def internal_mapper(a,n):
	li = []
	tmp = a.split(' - ')
	word = tmp[0]
	tmp_first = tmp[1].split(' & ')
	filename = tmp_first[0]
	N = tmp_first[1]
	mystring = str(word)+','+str(filename)+','+str(n)+","+str(N)
	print "Value of li is: %s" % mystring
	li.append(mystring)
	return li


def internal_mapper1(a,n):
	li = []
	tmp = a.split(' - ')
	word = tmp[0]
	tmp_first = tmp[1].split(' & ')
	filename = tmp_first[0]
	N = tmp_first[1]
	mystring = str(word)+','+str(filename)+','+str(n)+","+str(N)
	print "Value of li is: %s" % mystring
	li.append(mystring)
	return li

def internal_mapper3(a,n):
	li = []
	tmp = a.split(' - ')
	word = tmp[0]
	tmp_first = tmp[1].split(' & ')
	filename = tmp_first[0]
	N = tmp_first[1]
	mystring = str(word)+','+str(filename)+','+str(n)+","+str(N)
	print "Value of li is: %s" % mystring
	li.append(mystring)
	return li


def myfunction(filename,text):
	print "In the function lol"
	print "My RDD1 is %s" %  filename
	print "My RDD2 is %s" %  text



def mapper_reducer_new(filename,entire_text):
	current_word = None
	current_count = 0
	tmp_list={}
	item_list=[]
	print "Here"
	print "Text is : %s" % entire_text
	#removed_n = entire_text.replace("\n","")
	lines = entire_text.split("\n")

	#print "Reached here: %s" % lines
	
	for line in lines:
		temporary_var = line.split(",")
				
		word = temporary_var[0]
		#print word
		#tmp_list1 = temporary_var[1]#.split("&")
		#filename = tmp_list1[0]
		#print tmp_list1
#		tmp_list2 = tmp_list1[1].split("/t")
#		n = tmp_list2[0]
#		tmp_list3 = tmp_list2[1].split("/")
#		N = tmp_list3[0]
		count = 1
		
		try:
			count = int(count)
		except ValueError:
			continue
		
		if current_word == word:
			current_count += count
		else:
			if current_word:
				tmp_list[current_word]=current_count
			current_count = count
			current_word = word

	if current_word == word:
		tmp_list[current_word]=current_count

	print tmp_list
	my_newli=[]
	for line in lines:
		temporary_var = line.split(",")
		word = temporary_var[0]
		if(word!=""):
			new_line = line+","+str(tmp_list[word])
			my_newli.append(new_line)
		
	return my_newli


def mymapper4(x):
	D=10
	#tmp_var = a.split(',')
	#print tmp_var
	word = x.split(',')[0]
	if(word!=' '):
		filename = x.split(',')[1]
		n = float(x.split(',')[2])
		N = float(x.split(',')[3])
		m = float(x.split(',')[4])
		TFIDF = float(n/N * math.log(D/m))
		final_output = word+' - '+filename+' '+str(TFIDF)
		return final_output



count = spark_content.wholeTextFiles("input").flatMap(lambda (filename,text): mapper_reducer12(filename,text)) \
             .map(lambda a: (a.encode('utf-8'), 1)) \
             .reduceByKey(lambda a, b: a + b) \
	     .sortByKey() \
	     .flatMap(lambda (a,b): internal_mapper3(a,b)) #\
 	     #.map(lambda a: (a.split(',')[0],a.split(',')[1]+","+a.split(',')[2]+","+a.split(',')[3])) \
 	     #.map(lambda a: (a.split(',')[0]+'&'+a.split(',')[1], 1 )) \
	     #.reduceByKey(lambda x, y: x+y)

#rdd2 = spark_content.wholeTextFiles("input").flatMap(lambda (filename,text): mapper_reducer12(filename,text)) \
#             .map(lambda a: (a.encode('utf-8'), 1)) \
#             .reduceByKey(lambda a, b: a + b) \
#	     .flatMap(lambda (a,b): internal_mapper1(a,b)) \
# 	     .map(lambda a: (a.split(',')[0], 1 )) \
#	     .reduceByKey(lambda x, y: x+y)

#('direction', '1,file:/home/piyush/Downloads/spark_assignment/spark-2.0.0-bin-hadoop2.7/input/09.txt')
#file:/home/piyush/Downloads/spark_assignment/spark-2.0.0-bin-hadoop2.7/input/09.txt,million - 1
#count.collect()


#final_rdd = count.join(rdd2)



count.coalesce(1).saveAsTextFile("output")
#rdd2.saveAsTextFile("output2.txt")

print "Printed file:"



## Merging code
count = spark_content.wholeTextFiles("output/part-00000").flatMap(lambda (filename,text): mapper_reducer_new(filename,text.encode('utf-8'))) \
		     .map(lambda x: mymapper4(x))

count.coalesce(1).saveAsTextFile("output_piyushz")




#new_rdd = spark_content.wholeTextFiles("input_next").flatMap(lambda (filename,text): myfunction(filename,text))

#final_rdd.saveAsTextFile("output3.txt")

#count.coalesce(1).saveAsTextFile("output1.txt")
#global cnt
#print  "Count is : %s" % cnt
#global new_items
#print "New Li is %s" % new_items

#print "Text file is: %s" % text_file
#.map(lambda a: (a,1)).reduceByKey(lambda a,b:a+b)

#counts.coalesce(1).saveAsTextFile("output1.txt")
#filename = text_file.collect()
#print "text_file is %s" % filename[0]

#for row in filename:
	#print "Filename: %s" % line[0]
#   	file_name = row[0]
#	text_content = row[1]
#	counts = text_file.flatMap(lambda line: text_content.split(" ")) \
#             .map(lambda word: (word.encode('utf-8'), 1)) \
#             .reduceByKey(lambda a, b: a + b)

#	counts.coalesce(1).saveAsTextFile("output1.txt")

#counts = text_file.flatMap(lambda line: line.split(" ")) \
#             .map(lambda word: (word.encode('utf-8'), 1)) \
#             .reduceByKey(lambda a, b: a + b)


#counts.coalesce(1).saveAsTextFile("output1.txt")


#counts = text_file.flatMap(lambda line: line.split(" ")) \
#             .map(lambda word: (word.encode('utf-8'), 1)) \
#             .reduceByKey(lambda a, b: a + b)


#counts.coalesce(1).saveAsTextFile("output1.txt")





















