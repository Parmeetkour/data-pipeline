import json
from Connection.SourceToParsedHandlers.MYSQLHandler import MYSQLHandler

def get_connection_info(filename):
	f = open(filename)
	info=json.load(f)
	f.close()
	return info
def get_table_info(filename):
	f = open(filename)
	info=json.load(f)
	f.close()
	return info

if __name__=="__main__":
	conn_info=get_connection_info('C:\\Users\\hp\\Desktop\\PYTHON\\pipeline system\\Connection\\connectioninfo.json')
	table_info=get_table_info('C:\\Users\hp\\Desktop\PYTHON\\pipeline system\\table_config.json')
	conn=MYSQLHandler(conn_info)
	conn.do_source_to_landing(table_info)
	conn.do_landing_to_parsed(table_info)
	
	