#!/usr/bin/python
# -*- coding: utf-8 -*-

# this folder monitor
import sys
import os.path
import time
import MySQLdb
import codecs
import ConfigParser
import zipfile

import logging
import logging.handlers
import socket
import signal
import datetime
from BeautifulSoup import BeautifulSoup

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
# sys.stdin.encoding = 'UTF-8'
sys.stdout.encoding = 'UTF-8'
# sys.stderr.encoding = 'UTF-8'
root = "/mnt/sas-fs/input"
lastList={}
curList={}
lastList_size={}
curList_size={}
stableList={}
deleteList={}
firstcheck=True
hostname=socket.gethostname()

class TimedCompressedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
	"""
		Extended version of TimedRotatingFileHandler that compress logs on rollover.
		by Angel Freire <cuerty at gmail dot com>
	"""
	def doRollover(self):
		"""
		do a rollover; in this case, a date/time stamp is appended to the filename
		when the rollover happens.  However, you want the file to be named for the
		start of the interval, not the current time.  If there is a backup count,
		then we have to get a list of matching filenames, sort them and remove
		the one with the oldest suffix.

		This method is a copy of the one in TimedRotatingFileHandler. Since it uses

		"""
		self.stream.close()
		# get the time that this sequence started at and make it a TimeTuple
		t = self.rolloverAt - self.interval
		timeTuple = time.localtime(t)
		dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
		if os.path.exists(dfn):
			os.remove(dfn)
		os.rename(self.baseFilename, dfn)
		if self.backupCount > 0:
			# find the oldest log file and delete it
			s = glob.glob(self.baseFilename + ".20*")
			if len(s) > self.backupCount:
				s.sort()
				os.remove(s[0])
		#print "%s -> %s" % (self.baseFilename, dfn)
		if self.encoding:
			self.stream = codecs.open(self.baseFilename, 'w', self.encoding)
		else:
			self.stream = open(self.baseFilename, 'w')
		self.rolloverAt = self.rolloverAt + self.interval
		if os.path.exists(dfn + ".zip"):
			os.remove(dfn + ".zip")
		file = zipfile.ZipFile(dfn + ".zip", "w")
		file.write(dfn, os.path.basename(dfn), zipfile.ZIP_DEFLATED)
		file.close()
		os.remove(dfn)

log=logging.getLogger('main') 
log.setLevel(logging.DEBUG) 

formatter=logging.Formatter('%(asctime)s.%(msecs)d %(levelname)s in \'%(module)s\' at \
	line %(lineno)d: %(message)s','%Y-%m-%d %H:%M:%S') 
if log.level < 20:
	handler = TimedCompressedRotatingFileHandler('/var/log/videowf/folder_monitor.log', when='midnight', interval=1)
	# handler=logging.FileHandler('/var/log/videowf/folder_monitor.log', 'a') 
	handler.setFormatter(formatter)
	handler.setLevel(logging.DEBUG)
	log.addHandler(handler) 
if log.level > 10:
	formatter_info=logging.Formatter('%(asctime)s.%(msecs)d %(levelname)s in \'%(module)s\': %(message)s','%Y-%m-%d %H:%M:%S') 
	handler = TimedCompressedRotatingFileHandler('/var/log/videowf/folder_monitor.log', when='midnight', interval=1)
	# handler=logging.FileHandler('/var/log/videowf/folder_monitor.log', 'a') 
	handler.setLevel(logging.INFO) 
	handler.setFormatter(formatter_info) 
	log.addHandler(handler) 

handler=logging.handlers.SMTPHandler('gs-hcnlb-gs-slm.novy.tv',hostname+'@novy.tv',\
	'aignatiev@novy.tv','Server '+hostname+': Critical error found')
handler.setLevel(logging.CRITICAL) 
handler.setFormatter(formatter) 
log.addHandler(handler) 


def receive_signal(signum, stack):
	if signum in [1,2,3,8,9,15]:
		log.critical('Caught signal %s, exiting.' %(str(signum)))
		exitFlag=1
		# time.sleep(1)
		sys.exit(1)
	else:
		log.critical('Caught signal %s, ignoring.' %(str(signum)))
	pass

def getFiles():
	log.debug("Current Files:")
	# print "Current Files:"
	curList.clear()
	for path, subdirs, files in os.walk(root):
		for name in files:
			log.debug(str(os.path.join(path))+" "+str(name))
			# print os.path.join(path.decode(sys.stdin.encoding), name.decode(sys.stdin.encoding))
			if name != ".DS_Store" and name[:2]!="._" and not "lost+found" in path and not ".etc"\
			in path:
				log.debug(str(os.path.getmtime(path+"/"+name)))
				# print os.path.getmtime(path+"/"+name)
				if name is not None and path is not None:
					curList[path.decode('UTF-8')+"/"+name.decode('UTF-8')]=os.path.getmtime(path+"/"+name)
					curList_size[path.decode('UTF-8')+"/"+name.decode('UTF-8')]=os.path.getsize(path+"/"+name)

def compare_curList_with_lastList(dbip,dbuser,dbpass,dbname):
	log.debug("Compare curList and lastList:")
	# print "Compare curList and lastList:"
	for file in curList:
		log.debug("Check "+str(file.encode('utf-8'))+" in lastList:")
		# print "\nCheck "+file+" in lastList:"
		if lastList.has_key(file):
			log.debug("File exist. Compare mtime curList with lastList for this file.")
			# print "File exist. Compare mtime curList with lastList for this file."
			if curList[file] != lastList[file]:
				log.debug("mtime was changed. lastList=curList")
				# print "mtime was changed. lastList=curList"
				lastList[file]=curList[file]
				lastList_size[file]=curList_size[file]
			else:
				if int(curList_size[file]):
					log.debug("mtime was stable. Check file in stablelist")
					# print "mtime was stable. Check file in stablelist"
					if stableList.has_key(file):
						log.debug("File exist in stablelist")
						# print "File exist in stablelist"
						continue
					else:
						log.info(file)
						log.info("File not exist in stablelist. Add file to stablelist")
						# print "File not exist in stablelist. Add file to stablelist"
						new_filename=transliterate(file)
						if new_filename==file:
							stableList[file]=1
							if int(curList_size[file]) < 3000000:
								xml_statement=parse_fcp(file[:(file.rfind('/')+1)],file[(file.rfind('/')+1):],\
									dbip,dbuser,dbpass,dbname)
								if str(xml_statement) == "False":
									add_NewMedia_to_DB(file[:(file.rfind('/')+1)],file[(file.rfind('/')+1):], \
									dbip,dbuser,dbpass,dbname)
							else:
								add_NewMedia_to_DB(file[:(file.rfind('/')+1)],file[(file.rfind('/')+1):], \
                                                                        dbip,dbuser,dbpass,dbname)

						else:
							file=file.encode('UTF-8')
							log.debug("FILE!!!!: %s "  % file)
							os.rename(file,new_filename)

		else:
			log.debug("File not exist in lastList. Add to lastList")
			# print "File not exist in lastList. Add to lastList"
			lastList[file]=curList[file]
			lastList_size[file]=curList_size[file]
	pass

def compare_lastList_with_curList(dbip,dbuser,dbpass,dbname):
	log.debug("Compare lastList and curList:")
	# print "Compare lastList and curList:"
	for file in lastList:
		log.debug("Check file "+file+" in curList")
		# print "\nCheck file "+file+" in curList"
		if curList.has_key(file):
			log.debug("File exist in curList")
			# print "File exist in curList"
			continue
		else:
			log.info(file)
			log.info("File doesn't exist in curList.")
			# print "File doesn't exist in curList."
			deleteList[file]=""
	delete_remfile()
	pass

def delete_remfile():
	log.debug("Compare lastList and curList:")
	# print "Compare lastList and curList:"
	for file in deleteList:
		log.debug("Delete file "+file+" from lastList and stableList")
		# print "\nDelete file "+file+" from lastList and stableList"
		if file in stableList:
			del stableList[file]
		else:
		    pass
		if file in lastList:
			del lastList[file]
		else:
		    pass   
		if file in lastList_size:
			del lastList_size[file]
		else:
		    pass
		if not firstcheck:
			log.info("Say to DB: File was delete!")
			# print "Say to DB: File was delete!"
	deleteList.clear()
	pass

def get_init_files(dbip,dbuser,dbpass,dbname):
	db = MySQLdb.connect(dbip,dbuser,dbpass,dbname,\
			use_unicode=True, charset="utf8")
	cursor = db.cursor()
	sql = "SELECT filepath,filename,mtime FROM mediastore WHERE parentid is NULL"

# try:
	log.debug(sql)
	cursor.execute(sql)
	results = cursor.fetchall()
	for row in results:
		filepath = row[0]
		filename = row[1]
		mtime = row[2]
		log.debug(filepath+filename+"="+str(mtime))
		# print filepath+filename,"=",mtime
		lastList[filepath+filename]=mtime
		stableList[filepath+filename]=1

# except:
# 	print "Error: unable to fecth data"
	db.close()
	pass

def get_WF_for_NewMedia(path,cursor):
	cursor.execute ("SELECT dict_source.dict_wf,dict_source.mail_id,dict_source.id,dict_source.dict_wf_xml,\
	dict_source.mediatypeid,dict_mediatype.dest,dict_mediatype.ext FROM dict_source \
	LEFT JOIN dict_mediatype ON dict_mediatype.id=dict_source.mediatypeid \
	 WHERE inputfilepath='"+path+"'")
	row = cursor.fetchone()
	if row is None:
		log.info("WF for FILE not found")
		return int("0"),int("0"),int("0"),int("0"),int("0"),int("0"),int("0")
	else:
		log.info("WF for FILE: "+str(row[0]))
		# print "WF for FILE: ", row[0]
		log.info("MAIL ID for FILE: "+str(row[1]))
		# print "MAIL ID for FILE: ", row[1]
		return int(row[0]),int(row[1]),int(row[2]),int(row[3]),int(row[4]),str(row[5]),str(row[6])
	pass

def add_NewMedia_to_DB(path,filen,dbip,dbuser,dbpass,dbname):
	db = MySQLdb.connect(dbip,dbuser,dbpass,dbname,\
			use_unicode=True, charset="utf8")
	cursor = db.cursor()
	wf_data=get_WF_for_NewMedia(path,cursor)

	if int(wf_data[0]) != 0:
		sql = "INSERT INTO mediastore(filepath, filename, wfid, step, stepstatus, mediatype,\
			mtime, mail_id, root_path_id, dtadd)\
			VALUES('%s', '%s', '%d', '%d', '%d', '%d', '%f', '%d', '%d', now())" % \
	       (path, filen, int(wf_data[0]), 0, 1, 1,lastList[path+filen],int(wf_data[1]),int(wf_data[2]))
		log.debug(sql)
		# print "Add file to DB: ",sql
		try:
			cursor.execute(sql)
			db.commit()
			log.info("File added to DB and commit")
			# print "File added to DB and commit"
		except:
			db.rollback()
			log.exception("exception message")
			print "File NOT added to DB and ROLLBACK"
		db.close()
	pass

def transliterate(string):

    capital_letters = {u'А': u'A',
                       u'Б': u'B',
                       u'В': u'V',
                       u'Г': u'G',
                       u'Д': u'D',
                       u'Е': u'E',
                       u'Ё': u'E',
                       u'З': u'Z',
                       u'И': u'I',
                       u'Й': u'Y',
                       u'К': u'K',
                       u'Л': u'L',
                       u'М': u'M',
                       u'Н': u'N',
                       u'О': u'O',
                       u'П': u'P',
                       u'Р': u'R',
                       u'С': u'S',
                       u'Т': u'T',
                       u'У': u'U',
                       u'Ф': u'F',
                       u'Х': u'H',
                       u'Ъ': u'',
                       u'Ы': u'Y',
                       u'Ь': u'',
                       u'Э': u'E',
                       u'Є': u'E',
                       u'І': u'I',}

    capital_letters_transliterated_to_multiple_letters = {u'Ж': u'Zh',
                                                          u'Ц': u'Ts',
                                                          u'Ч': u'Ch',
                                                          u'Ш': u'Sh',
                                                          u'Щ': u'Sch',
                                                          u'Ю': u'Yu',
                                                          u'Ї': u'Yi',
                                                          u'Я': u'Ya',}


    lower_case_letters = {u'а': u'a',
                       u'б': u'b',
                       u'в': u'v',
                       u'г': u'g',
                       u'д': u'd',
                       u'е': u'e',
                       u'ё': u'e',
                       u'ж': u'zh',
                       u'з': u'z',
                       u'и': u'i',
                       u'й': u'y',
                       u'к': u'k',
                       u'л': u'l',
                       u'м': u'm',
                       u'н': u'n',
                       u'о': u'o',
                       u'п': u'p',
                       u'р': u'r',
                       u'с': u's',
                       u'т': u't',
                       u'у': u'u',
                       u'ф': u'f',
                       u'х': u'h',
                       u'ц': u'ts',
                       u'ч': u'ch',
                       u'ш': u'sh',
                       u'щ': u'sch',
                       u'ъ': u'',
                       u'ы': u'y',
                       u'ь': u'',
                       u'э': u'e',
                       u'ю': u'yu',
                       u'я': u'ya',
                       u'є': u'e',
                       u'ї': u'yi',
                       u'’': u'',
                       u'\'': u'',
                       u'»': u'',
                       u'«': u'',
                       u'–': u'-',
                       u'і': u'i',}

    capital_and_lower_case_letter_pairs = {}

    for capital_letter, capital_letter_translit in capital_letters_transliterated_to_multiple_letters.iteritems():
        for lowercase_letter, lowercase_letter_translit in lower_case_letters.iteritems():
            capital_and_lower_case_letter_pairs[u"%s%s" % (capital_letter, lowercase_letter)] = u"%s%s" % (capital_letter_translit, lowercase_letter_translit)

    for dictionary in (capital_and_lower_case_letter_pairs, capital_letters, lower_case_letters):

        for cyrillic_string, latin_string in dictionary.iteritems():
            string = string.replace(cyrillic_string, latin_string)

    for cyrillic_string, latin_string in capital_letters_transliterated_to_multiple_letters.iteritems():
        string = string.replace(cyrillic_string, latin_string.upper())

    return string

def parse_fcp(fcp_path,fcp_filename,dbip,dbuser,dbpass,dbname):
	with open(fcp_path+fcp_filename, 'r') as file:
		first_line = file.readline()
		# print first_line
		if "xml version=" in first_line:
			log.info("This is XML")
			# print "This is XML"
			segmentList=[]
			# file = '/mnt/sas-fs/input/web/for_cutting/test_01.xml'
			handler = file
			soup = BeautifulSoup(handler)
			# soup = BeautifulSoup(html_doc)
			# print(soup.prettify())
			print "filename: ",soup.find('video').find('track').find('file').find('name').string
			seg_filename=soup.find('video').find('track').find('file').find('name').string
			print "pathurl: ",soup.find('video').find('track').find('file').find('pathurl').string,"\n"
			seg_filepath=soup.find('video').find('track').find('file').find('pathurl').string[:-len(seg_filename)]
			seg_filepath = "/mnt/sas-fs/input/web/"+seg_filepath[len("file://localhost/Volumes/web-input/"):]
			print "Filepath: ",seg_filepath
			clips=soup.findAll('clip')
			for clip in clips:
				clip_id=clip['id']
				print clip['id']
				clip_name=clip['id'].rstrip()
				print "name: !"+str(clip_name)+"!"
				clip_in=clip.find('in').string

				print "in: ", clip.find('in').string
				clip_out=clip.find('out').string
				print "out: ", clip.find('out').string
				clip_dur=clip.find('duration').string
				print "duration: ", (int(clip_out)-int(clip_in))
				print "TC IN: ",str(datetime.timedelta(milliseconds=int(clip_in)*40))
				tc_in=str(datetime.timedelta(milliseconds=int(clip_in)*40))
				print "TC DUR:",str(datetime.timedelta(milliseconds=int(int(clip_out)-int(clip_in))*40))
				tc_dur=	str(datetime.timedelta(milliseconds=int(int(clip_out)-int(clip_in))*40))
				# print "duration: ", clip.find('duration').string,"\n"
				# print clip
				markers=clip.findAll('marker')
				# print "\n",markers,"\n"
				for marker in markers:
					snapshot=marker.find('in').string
					# print "marker: ",snapshot,"\n"

				# 	snapshot=marker.find('in')
					if int(snapshot) >= int(clip_in) and int(snapshot) <= int(clip_out):
						print "marker: ",str(datetime.timedelta(milliseconds=int(snapshot)*40)),"\n"
						tc_snapshot=str(datetime.timedelta(milliseconds=int(snapshot)*40))
						break
				if snapshot is None:
					segmentList.append([str(seg_filepath), str(seg_filename), str(clip_name), \
						str(tc_in), str(tc_dur)])
				else:
					segmentList.append([str(seg_filepath), str(seg_filename), str(clip_name), \
						str(tc_in), str(tc_dur), str(tc_snapshot)])
			add_Segment_to_DB(segmentList,dbip,dbuser,dbpass,dbname,fcp_path,fcp_filename)
			# print segmentList
			# print segmentList[0]
			os.remove(fcp_path+fcp_filename)  
			return "True"
		# os.remove(fcp_path+fcp_filename)  
		return "False"

def add_Segment_to_DB(segmentList,dbip,dbuser,dbpass,dbname,fcp_path,fcp_filename):
	db = MySQLdb.connect(dbip,dbuser,dbpass,dbname,\
			use_unicode=True, charset="utf8")
	cursor = db.cursor()
	# wf_data=get_WF_for_NewMedia("/mnt/sas-fs/input/web/mov/4x3/",cursor)
	wf_data=get_WF_for_NewMedia(fcp_path,cursor)
	print "wf_data: ",wf_data
	print "segmentList:",segmentList
	if int(wf_data[0]) != 0:
		try:

			sql = "INSERT INTO mediastore(filepath, filename, wfid, step, stepstatus, mediatype,\
				mtime, mail_id, root_path_id, dtadd)\
				VALUES('%s', '%s', '%d', '%d', '%d', '%d', '%f', '%d', '%d', now())" % \
		       (segmentList[0][0], segmentList[0][1], 2410, 0, 1, 1,lastList[fcp_path+fcp_filename],int(wf_data[1]),int(wf_data[2]))
			log.debug(sql)
			print "Add file to DB: ",sql
			cursor.execute(sql)
			# db.commit()

			sql_get_asset_id="SELECT id FROM mediastore WHERE mtime='%f'" % (lastList[fcp_path+fcp_filename])
			print "sql_get_asset_id:",sql_get_asset_id
			cursor.execute(sql_get_asset_id)
			resultss = cursor.fetchall()
			db.commit()
			print resultss
			# parent_id=100500
			for row in resultss:
				parent_id = row[0]
			info_id=0
			while info_id == 0:
				sql_get_info_stat="SELECT status FROM query_actions WHERE mediastore_id='%d' and action_type='info'" % (parent_id)
				print "sql_get_info_stat: ",sql_get_info_stat
				cursor.execute(sql_get_info_stat)
				info_status = cursor.fetchall()
				db.commit()
				print info_status
				if info_status is not None:
					for info_stat in info_status:
						# parent_id = info_stat[0]
						print "info_stat[0]:",info_stat[0]
						if info_stat[0] is not None:
							if int(info_stat[0]) != 0:
								info_id=info_stat[0]
				# 		info_id = info_status[0]
					# if info_status[0] is not None:
					# 	print "info_status[0]: ",info_status[0]
					# print info_status
				# 	if int(info_status[0]) != 0:
				# 		info_id = info_status[0]
				print "info_id: ",info_id
				time.sleep(1)
			if int(info_id) > 0:
				for segment in segmentList:
					print "segment: ",segment
					# print parent_id,parent_id, wf_data[5],segment[1]+"."+wf_data[6],wf_data[3],wf_data[4],\
					# 	 	int(wf_data[1]),int(wf_data[2])
					sql_add_segment = "INSERT INTO mediastore (parentid,rootid,filepath,filename, \
						wfid, step, stepstatus,mediatype,dtadd,mail_id,root_path_id,segment_in,segment_dur,\
						segment_snapshot) \
						VALUES ('%d','%d','%s','%s','%d',0,1,'%d',now(),'%d','%s','%s','%s','%s')" % \
						 (parent_id,parent_id, wf_data[5],segment[2]+"."+wf_data[6],wf_data[3],wf_data[4],\
						 	int(wf_data[1]),int(wf_data[2]),str(segment[3]),str(segment[4]),str(segment[5]))
					print "sql_add_segment: ",sql_add_segment
					cursor.execute(sql_add_segment)		 
			# slq_upd_parent="UPDATE mediastore SET wfstatus= where id='%d'" % \	(id)
			db.commit()
			# db.rollback()

			log.info("File added to DB and commit")
			print "File added to DB and commit"
		except:
			db.rollback()
			log.exception("exception message")
			print "File NOT added to DB and ROLLBACK"
	db.close()
	pass

def check_upd(dbip,dbuser,dbpass,dbname,module_name):
	db = MySQLdb.connect(dbip,dbuser,dbpass,dbname,\
			use_unicode=True, charset="utf8")
	cursor = db.cursor()
	sql_upd_status = "SELECT %s FROM dict_upd WHERE hostname='%s'" % (str(module_name),str(hostname))
	log.info("sql_upd_status: %s" % str(sql_upd_status))
	cursor.execute(sql_upd_status)
	upd_res = cursor.fetchall()
	upd_status = upd_res[0][0]
	log.info("upd_status: %s" % str(upd_status))
	if int(upd_status):
		log.info("Restart module!!!")
		sql_upd_status_set="UPDATE dict_upd SET %s='0' WHERE hostname='%s'" % (str(module_name),str(hostname))
		cursor.execute(sql_upd_status_set)
		log.info("sql_upd_status: %s" % str(sql_upd_status_set))
		db.commit()
		db.close()
		sys.exit(1)
	db.close()
	pass

def main():
	uncatchable = ['SIG_DFL','SIGSTOP','SIGKILL']
	for i in [x for x in dir(signal) if x.startswith("SIG")]:
		if not i in uncatchable:
			signum = getattr(signal,i)
			signal.signal(signum,receive_signal)
	config = ConfigParser.ConfigParser()
	# config.read('main.conf')
	config.read('/usr/local/sbin/videowf/main.conf')
	dbip=config.get("mysql", "ip")
	dbuser=config.get("mysql", "user")
	dbpass=config.get("mysql", "password")
	dbname=config.get("mysql", "db")
	
	get_init_files(dbip,dbuser,dbpass,dbname)
	while (True):
		try:
			log.info("Wake UP")
			# print "\n\nWake UP"
			getFiles()
			log.debug("curList: ")
			# print "curList: "
			for file in curList:
				log.debug(str(file.encode('utf-8'))+" "+str(curList[file])+"\t"+str(curList_size[file]))
				# print str(file)+" "+str(curList[file])+"\t"+str(curList_size[file])
				# print file, curList[file]
			log.debug("lastList: ")
			# print "lastList: "
			for file in lastList:
				log.debug(str(file.encode('utf-8'))+" "+str(lastList[file]))
				# print file, lastList[file]
			compare_curList_with_lastList(dbip,dbuser,dbpass,dbname)
			compare_lastList_with_curList(dbip,dbuser,dbpass,dbname)
			# log.debug("stableList: ")
			# # print "stableList: "
			# for file in stableList:
			# 	log.debug(str(file)+" "+str(stableList[file]))
			# 	# print file, stableList[file]
			# 	#print file[:file.rfind('/')]
			# log.debug("deleteList: ")
			# # print "deleteList: "
			# for file in deleteList:
			# 	log.debug(str(file)+" "+str(deleteList[file]))
			# 	# print file, deleteList[file]
			firstcheck=False
			check_upd(dbip,dbuser,dbpass,dbname,"fm")
			time.sleep(5)
		except:
			log.exception("exception message")
			# log.critical("Program Exit. You can read error in log file")
			sys.exit(1)
		pass
	pass

if __name__ == '__main__':
	try:
		pid = os.fork()
		if pid > 0:
			sys.exit(0)
	except OSError, e:
		sys.exit(1)

	try:
		pid = os.fork()
		if pid > 0:
			sys.exit(0)
	except OSError, e:
		sys.exit(1) 
		
	main()
