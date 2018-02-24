#!/usr/bin/env python
#encoding:utf8

from ConfigParser import ConfigParser #读取配置文件 ini.inf
import os
import MySQLdb

#MySQLDConfig 默认配置文件字典，修改配置文件选项
#解析创建配置文件
#
def getMyVariables(cur):
	cur.execute('show global variables')
	data = cur.fetchall()
	return dict(data)

class MySQLDConfig(ConfigParser):
	def __init__(self,config,**kw):
	#	super(MySQLConfig,self).__init__(allow_no_value=True)
		ConfigParser.__init__(self, allow_no_value=True)
		self.config = config
		self.mysqld_vars = {}
		if os.path.exists(self.config):
			self.read(self.config)
			self.get_mysqld_vars()
		else:
			self.set_mysqld_defaults_vars()
		self.set_mysqld_vars(kw)

	def set_mysqld_vars(self,kw):
		for k,v in kw.items():
			#setattr(self,k,v)
			self.mysqld_vars[k] = v

	def get_mysqld_vars(self):
		options = self.options('mysqld')
		rst = {}
		for i in options:
			rst[i] = self.get('mysqld', i)
		self.set_mysqld_vars(rst)
	
	def set_var(self,k,v):
		self.mysqld_vars[k] = v

	def set_mysqld_defaults_vars(self):
		defaults = {
		"query_cache_size":"16M", 
		"binlog_format":"mixed", 
		"socket":"/var/lib/mysql/mysql.sock", 
		"table_open_cache":"256", 
		"key_buffer_size":"256M", 
		"sort_buffer_size":"1M", 
		"server-id":"1", 
		"thread_concurrency":"8", 
		"myisam_sort_buffer_size":"64M", 
		"max_allowed_packet":"1M", 
		"skip-external-locking":None,
		"datadir":"/tmp/mysql101", 
		"read_buffer_size":"1M", 
		"log-bin":"mysql-bin", 
		"max_connection":"200", 
		"thread_cache_size":"8", 
		"port":"3307", 
		"read_rnd_buffer_size":"4M", 
		}
		self.set_mysqld_vars(defaults)	

	def save(self):
		if not self.has_section('mysqld'):
			self.add_section('mysqld')
		for k,v in self.mysqld_vars.items():
			self.set('mysqld',k,v)
		with open(self.config,'w') as fd:
			self.write(fd)		

if __name__ == '__main__':
	mc = MySQLDConfig('/tmp/my01.cnf', max_connection=200)
	mc.set_var('skip-slave-start',None)
	mc.save()
	print mc.get('mysqld','datadir') 
	#print mc.max_connection

