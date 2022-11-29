from Connection.SourceToParsedHandlers.BaseHandler import BaseHandler
import shutil
import time
import os
import traceback
import logging
import pymysql
from datetime import date
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import csv
class MYSQLHandler(BaseHandler):

	def __init__(self, kwargs):
        # Default connection Details
		self.temp_file = kwargs['temp_file']
		self.host = kwargs['host']
		self.user=kwargs['user']
		self.password=kwargs['password']
		self.port=kwargs['port']
		self.database=kwargs['database']
		
	
	def get_mysql_connection(self):
		"""This function will return Mysql Cursor and Connection Object"""
		try:
			
			print("----*******************888")
			db_conn=pymysql.connect(host=self.host,user=self.user,password=self.password,database=self.database)
			cur=db_conn.cursor()
			
		except:
			print(traceback.format_exc())
			if db_conn:
				db_conn.rollback()
			
			
	
		else:
			print('######Succesfully Create the Cursor and Connection')
			return db_conn,cur
		
	def create_dir(self, temp_dir) -> None:
		"""
       			 Deletes a temporary directory alongwith its content if it exists and then create it again
        		:param temp_dir: temporary directory location
        		:return:
		"""
		if os.path.exists(temp_dir):
			shutil.rmtree(temp_dir)
		os.makedirs(temp_dir, exist_ok=True)
		print("##### Dir created...")
	def write_to_csv(self, cursor, file) -> None:
		"""
        	Writes database cursor to a local file
        	:param cursor: database cursor
        	:param file: local file to write to
       	 	:return:
		"""
		with open(file, 'w',newline='') as file_in:
			my_file = csv.writer(file_in,delimiter="," )
			header = [i[0] for i in cursor.description]
			my_file.writerow(header)
			record_number = int(2)
			data_rows = cursor.fetchmany(record_number)
			while len(data_rows) > 0:
				my_file.writerows(data_rows)
				data_rows = cursor.fetchmany(record_number)
		
	
	def do_source_to_landing(self, kwargs):
		conn=self.get_mysql_connection()
		table_name=kwargs['mysql_data_ingestion_config']['source_tablename']['MYSQL']
		tmp_location=kwargs['mysql_data_ingestion_config']['default_args']['dir_location']
		today = date.today()
		dd=today.strftime("%d-%m-%Y")
		dirname=tmp_location+str(dd)
		self.create_dir(dirname )
		cursor=conn[1]
		for table in table_name:
			query=f"Select * from {table}"
			cursor.execute(query)
			file=dirname+"\\"+table+".csv"
			print(file)
			print(f"#### Write Process Started for...{table}")
			self.write_to_csv( cursor, file)
			time.sleep(2)
			print(f"### Write Process Completed...{table}")
		else:
			cursor.close()
#----------------------Google drive write process		
	def list_files(self,dir_path):
		res = []
		# Iterate directory
		for dirpath,_,filenames in os.walk(dir_path):
			for f in filenames:
				res.append( os.path.abspath(os.path.join(dirpath, f)))
		return res
	def create_folder_drive(self,folder_name,drive):
		folder = drive.CreateFile({'title' : folder_name, 'mimeType' : 'application/vnd.google-apps.folder','parents':[{'id':'14oTD3B1EWYV4Zhe56pKhmMfKM7U4SvJS'}]})
		
		folder.Upload()
		

	def get_parant_id(self,drive,folder_name):
		file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
		#file_list =drive.ListFile({'q': f"title contains 'Project_Data/19-08-2022'"})
		
		for file in file_list:
			#print(f"Title-- {file['title']}  Id--{file['id']}, Modified Date--{date}")
			#return file[0]['id']
			print(file[0])
	def upload_file_to_specific_folder(self, folder_id, file_name,drive):
		fname=str(file_name).split("\\")
		fname=fname[-1]+"_"+fname[-2]
		file_metadata = {'title':fname , "parents": [{"id": folder_id, "kind": "drive#childList"}]}
		folder = drive.CreateFile(file_metadata)
		folder.SetContentFile(file_name) #The contents of the file
		folder.Upload()		
	def get_folder_id(self,date_folder_name,drive):
		folder_list = drive.ListFile({'q': "trashed=false"}).GetList()
		for folder in folder_list:
			if folder['title']==date_folder_name:
				return folder['id']
         		
 	
	def do_landing_to_parsed(self, kwargs):
		gauth = GoogleAuth()
		gauth.LocalWebserverAuth() # client_secrets.json need to be in the same directory as the script
		drive = GoogleDrive(gauth)
		print(drive)
		tmp_location=kwargs['mysql_data_ingestion_config']['default_args']['dir_location']
		today = date.today()
		dd=today.strftime("%d-%m-%Y")
		dir_path=tmp_location+str(dd)
		file_list=self.list_files(dir_path)
		self.create_folder_drive(str(dd),drive)
		date_folder=f'Project_Data/{dd}'
		folder_id=self.get_folder_id(str(dd),drive)
		print(date_folder)
		for file_name in file_list:
			print(f"Uplaod File....{file_name}....in....{date_folder}")
			time.sleep(2)
			self.upload_file_to_specific_folder(folder_id, file_name,drive)
		
		
		

