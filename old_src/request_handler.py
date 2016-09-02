import MySQLdb
import re, os, sys, time, types
import urllib

import flviz_globals as fg

class coord:
    """coordinate class

    lat - latitude (float)
    lon - longitude (float)"""
	def __init__(self, lat, lon):
		self.lat = lat
		self.lon = lon

class grabJob:
    """grabJob is a class to describe jobs coordinates and ancillary data

    some of the methods help in formatting requests to the flickr server"""
	def __init__(self, fromDate, toDate, ulCoord, lrCoord, pagesDone, totalPages, jobID):
        """__init__ takes a number of arguments:

        fromDate - starting time of pull (MySQL DATE)
        toDate - ending time of pull (MySQL DATE)
        coords - upper left & lower right 'coord' types
        pagesDone - pages reported completed (int)
        totalPages - total pages to do (int)
        jobID - MySQL key to job (int)"""
		self.fromDate = fromDate
		self.toDate = toDate
		self.ulCoord = ulCoord
		self.lrCoord = lrCoord
		self.pagesDone = pagesDone
		self.totalPages = totalPages
		self.jobID = jobID
	def apiBox(self):
        """apiBox formats and returns a string representing the upper
        left and lower right of the job."""
		minLat = str(min(self.ulCoord.lat, self.lrCoord.lat))
		maxLat = str(max(self.ulCoord.lat, self.lrCoord.lat))
		minLon = str(min(self.ulCoord.lon, self.lrCoord.lon))
		maxLon = str(max(self.ulCoord.lon, self.lrCoord.lon))
		retString = minLon+","+minLat+","+maxLon+","+maxLat
		return(retString)
	def jobBox(self):
        """jobBox presents a human-readable job boundary box for the logfiles
        other output."""
		retString =  "("+str(self.ulCoord.lat)+", "+str(self.ulCoord.lon)+", "
		retString += str(self.lrCoord.lat)+", "+str(self.lrCoord.lon)+")"
		return(retString)

def mysql_connect():
	"""returns a [cursor, connection] pair"""
	try:
        conn = MySQLdb.connect(host=fg.mysqlhost, user=fg.mysqluser, \
                                passwd=fg.mysqlpasswd, db=fg.mysqldb)
    except MySQLdb.Error, e:
		log("Error %d: %s" % (e.args[0]+e.args[1]))
		sys.exit(1)
	return(dict({'cursor': conn.cursor(), 'connection':conn}))

def getNegativeStatusList(conObject):
	"""get a list of jobs with negative status IDs - meaning work has not yet
    started on them.

    conObject is a full [cursor, conn] pair
    returns a list of 'jobJects'"""
	negativeQuery = "SELECT * FROM fv2_requests WHERE pagesdone<0 ORDER BY req_id ASC"
	conObject['cursor'].execute(negativeQuery)
	resultSet = conObject['cursor'].fetchall()
	jobList = []
	for row in resultSet:
		jobList.append(grabJob(row[1], row[2], coord(row[3], row[4]), coord(row[5], row[6]), \
									  row[8], row[7], row[0]))
	return(jobList)

def getWorkableJobsList(conObject):
	"""get a list of workable jobs, ordered by request id (asc)

    conObject is a full [cursor, conn] pair
    returns a list of 'jobJects'"""
	positiveQuery = "SELECT * FROM fv2_requests WHERE pagesdone>=0 ORDER BY req_id ASC"
	conObject['cursor'].execute(positiveQuery)
	resultSet = conObject['cursor'].fetchall()
	jobList = []
	for row in resultSet:
		log(row)
		jobList.append(grabJob(row[1], row[2], coord(row[3], row[4]), coord(row[5], row[6]), \
									  row[8], row[7], row[0]))
	return(jobList)

def updateNegativeJob(conObject, job):
    """updateNegativeJob starts work on a job.  It finds the number of pages (work to be done)
    and returns this number to the handler, which can then allocate more work to be done here.

    conObject - [cursor, conn] pair
    job - jobID
    returns number of pages to be completed, -1 on fail."""
	apikey = fg.apikey
	bbox = job.apiBox()
	log(bbox)
	queryURL = "http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key="
	queryURL += apikey+"&min_upload_date="+job.fromDate.strftime("%Y-%m-%d")
	queryURL += "&max_upload_date="+job.toDate.strftime("%Y-%m-%d")
	queryURL += "&bbox="+bbox+"&has_geo=1&accuracy=6&extras=tags,date_upload,geo&per_page=250"
	log(queryURL)

	k = urllib.urlopen(queryURL)
	time.sleep(4)
	doclines = k.read().split("\n")
	for line in doclines:
		if(line.find("pages=")>0):
			log(line)
			linesegs = line.split(" ")
			for seg in linesegs:
				if(seg.split("=")[0]=="pages"):
					newpages = seg.split("=")[1].replace("\"", "")
	if(type(newpages)!=types.NoneType):
		log("Job report for "+str(job.jobID)+": "+str(newpages))
		updateQuery = "UPDATE fv2_requests SET totalpages="+str(newpages)+", "
		updateQuery+= " pagesdone=0 WHERE req_id="+str(job.jobID)
		conObject['cursor'].execute(updateQuery)
		resultSet = conObject['cursor'].fetchall()
		return(newpages)
	else:
		return(-1)

def parseJobPage(jobJect)
	"""parseJobPage - parses a page given by the jobject, requires a valid page number

    jobJect - a valid job object"""
    apikey = fg.apikey
	job = jobJect
    bbox = job.apiBox()
	log(bbox)
	queryURL = "http://api.flickr.com/services/rest/?method=flickr.photos.search&api_key="
	queryURL += apikey
	queryURL += "&min_upload_date="+job.fromDate.strftime("%Y-%m-%d")
	queryURL += "&max_upload_date="+job.toDate.strftime("%Y-%m-%d")
	queryURL += "&bbox="+bbox+"&has_geo=1&accuracy=6&extras=tags,date_upload,geo,date_taken&per_page=250"
	queryURL += "&page="+str(jobJect.pagesDone+1)
	log(queryURL)

	inPage = urllib.urlopen(queryURL)
	time.sleep(2)

	docLines = inPage.read().split("\n")
	printDict = dict()
	lCount = 0
	retPage = []
	nonQuotedColumns = set(('latitude', 'longitude', 'accuracy', 'dateupload'))
	databaseColumns = set(('id', 'owner', 'secret', 'server', 'farm', 'title', \
						  'dateupload', 'latitude', 'longitude', 'accuracy', \
						  'place_id', 'woeid', 'tags', 'datetakengranularity', \
						  'datetaken'))

	for line in docLines:
		if(line.find("<photo id=")>0):
			ingestDict = dict()
			line = line.strip().replace("<photo ", "").replace(" />", "")
			lpieces = line.split("\"")
			for i in range(0, len(lpieces)):
				if(lpieces[i].find("=")>0):
					ingestDict[lpieces[i].replace("=", "").strip()] = lpieces[i+1]
					i+=1
		    query  = "INSERT INTO fv2_image_attr ("
			ingestDict['latitude'] = float(ingestDict['latitude'])
			ingestDict['longitude'] = float(ingestDict['longitude'])
			ingestDict['accuracy'] = int(ingestDict['accuracy'])
			ingestDict['dateupload'] = "FROM_UNIXTIME("+ingestDict['dateupload']+")"
			valueQuery = ""
			columnQuery = ""
		for col in ingestDict:
				if(col in databaseColumns):
					if(col not in nonQuotedColumns):
						ingestDict[col] = ingestDict[col].replace("'", "")
						valueQuery+="'"+ingestDict[col]+"', "
					else:
						valueQuery += str(ingestDict[col])+", "
					columnQuery+=str(col)+", "
			valueQuery+=str(job.jobID)
			columnQuery+="collect_note"
			query+=columnQuery+") VALUES ("+valueQuery+")"
			lCount+=1
			retPage.append([line, ingestDict, query])
	return(retPage)
	## returns a list of flickrJects

def insertFlickrLines(conObject, flickrJects):
	"""for each flickrJect, inserts it into the database

    conOject - [cursor, connection] pair
    flickrJects - array of [line, ingestDict, query]
    returns 1 for success, -1 fail; logs MySQL errors"""
	for line in flickrJects:
		try:
			conObject['cursor'].execute(line[2])
		#resultSet = conObject['cursor'].fetchall()
		except MySQLdb.Error, e:
			log(str(line[2]))
			log(str(e.args[0])+": "+str(e.args[1]))
			return(-1)
	return(1)

def incrementPageCountTo(conObject, jobID, newPages):
    """Increases pages completed.

    conObject - [cursor,connection] pair
    jobID - current job ID (int)
    newPages - new page count
    returns - (new) number of pages complete."""
	updateQuery = "UPDATE fv2_requests SET pagesdone="+str(newPages)+" WHERE req_id="+str(jobID)
	log(updateQuery)
	newPagesDone = 0
	try:
		conObject['cursor'].execute(updateQuery)
		newPageNumQuery = "SELECT pagesdone FROM fv2_requests WHERE req_id="+str(jobID)
		conObject['cursor'].execute(newPageNumQuery)
		resultSet = conObject['cursor'].fetchall()
		try:
			newPagesDone = resultSet[0][0]
		except:
			newPagesDone = -2
	except MySQLdb.Error, e:
		log(updateQuery)
		log(str(e.args[0])+": "+e.args[1])
		return(-1)
	return(newPagesDone)

def log(msg):
	"""logging function - uses a file for output rather than print statements"""
    outputLog.write(str(msg)+"\n")

logDir = fg.logDir
outputLog = file(logDir+str(time.time()).split(".")[0]+"_pygrablog.txt", 'w')

def main():
    """main runner for program.

    Performs a single overarching task: process new lines of flickr data into db
    Steps:
    1. Retrieve new joblist (negative statuses)
    2. Update negative statuses to positive pagecounts
    3. In order of job entry, lowest to highest, download new data for each job.
    4. Upon 3500 seconds of runtime, close connections and wait for new cron run.
    """
	print time.asctime()+" - starting"
	startTime = time.time()
	log(time.asctime()+" - Retrieving negative statuses, connecting, etc...")
	conObject = mysql_connect()
	negStatuses = getNegativeStatusList(conObject)
	for i in range(0, len(negStatuses)):
		log("Updating jobID "+str(negStatuses[i].jobID)+" "+negStatuses[i].jobBox())
		negStatuses[i].totalPages = updateNegativeJob(conObject, negStatuses[i])
		if(negStatuses[i].totalPages<0):
			log("Error updating "+str(negStatuses[i].jobID))
		else:
			log(str(negStatuses[i].jobID)+" successfully updated with "+str(negStatuses[i].totalPages))
	if(len(negStatuses)==0):
		log("There were no jobs in the 'beginning' queue; moving on to current jobs:")
	workableJobs = getWorkableJobsList(conObject)
	for job in workableJobs:
		if((time.time()-startTime) > 3500):
			break
		jCount = 0
		while(job.pagesDone<=job.totalPages):
			if((time.time()-startTime) > 3500):
				break;
			jCount+=1
			newFlickrLines = parseJobPage(job)
			if(insertFlickrLines(conObject, newFlickrLines)==1):
				log("insert complete")
				updatedProperly = incrementPageCountTo(conObject, job.jobID, job.pagesDone+1)
				if(updatedProperly > 0):
					log("properly updated!")
					job.pagesDone+=1
				else:
					log("error updating pagecount val: "+str(updatedProperly))
			else:
				log("issues with inserting a page")
	log(time.asctime()+"done")
	conObject['cursor'].close()
	conObject['connection'].commit()
	conObject['connection'].close()

if(__name__=="__main__"):
	main()
