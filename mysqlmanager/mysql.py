#!/usr/bin/env python

from ConfigParser import ConfigParser
import os
import MySQLdb

def getMyVariables(cur):
	cur.execute('show global variables')
	data = cur.fetchall()
	return dict(data)

class MySQLDConfig(ConfigParser):
	def __init__(self, config, **kw):
		#super(MySQLDConfig, self).__init__(allow_no_value=True)
		ConfigParser.__init__(self, allow_no_value=True)
		self.config = config
		self.mysqld_vars = {}
		if os.path.exists(self.config):
			self.read(self.config)
			self.get_mysqld_vars()
		else:
			self.set_mysqld_defaults_vars()
		self.set_mysqld_vars(kw)

	def set_mysqld_vars(self, kw):
		for k, v in kw.items():
			setattr(self, k ,v)
			self.mysqld_vars[k] = v

	def get_mysqld_vars(self):
		options = self.options('mysqld')
		rst = {}
		for o in options:
			rst[o] = self.get('mysqld', o)
		self.set_mysqld_vars(rst)

	def set_var(self, k, v):
		self.mysqld_vars[k] = v

	def set_mysqld_defaults_vars(self):
		defaults = {
			"query_cache_size": "16M",
			"binlog_format": "mixed",
			"socket": "/var/lib/mysql/mysql.sock",
			"thread_cache_size": "8",
			"sort_buffer_size": "1M",
			"server-id": "1",
			"datadir": "/tmp/mysql01",
			"myisam_sort_buffer_size": "64M",
			"max_allowed_packet": "1M",
			"skip-external-locking": None,
			"thread_concurrency": "8",
			"read_buffer_size": "1M",
			"port": "3306",
			"log-bin": "mysql-bin",
			"max_connection": "200",
			"table_open_cache": "256",
			"key_buffer_size": "256M",
			"read_rnd_buffer_size": "4M",
		}
		self.set_mysqld_vars(defaults)

	def save(self):
		if not self.has_section('mysqld'):
			self.add_section('mysqld')
		for k, v in self.mysqld_vars.items():
			self.set('mysqld', k, v)
		with open(self.config, 'w') as fd:
			self.write(fd)

if __name__ == '__main__':
	mc = MySQLDConfig('/tmp/my03.cnf', max_connection=200)
	mc.set_var('skip-slave-start', None)
	mc.save()
	print mc.get('mysqld', 'datadir')
	print mc.max_connection
