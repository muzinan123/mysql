#!/usr/bin/env python

import os
import sys
import MySQLdb
import time

DIRNAME = os.path.dirname(__file__)
OPSTOOLS_DIR = os.path.abspath(os.path.join(DIRNAME, '..'))
sys.path.append(OPSTOOLS_DIR)

from library.mysql import MySQLDConfig, getMyVariables
from optparse import OptionParser
from subprocess import Popen, PIPE

MYSQL_DATA_DIR = '/var/mysqlmanager/data'
MYSQL_CONF_DIR = '/var/mysqlmanager/conf'
MYSQL_BACK_DIR = '/var/mysqlmanager/backup'

REPLICATION_USER = 'repl'
REPLICATION_PASS = '123'

def opts():
	parser = OptionParser(usage="usage %prog options")
	parser.add_option("-n", "--name",
					dest="name",
					default="mysqlinstance",
					action="store",)
	parser.add_option("-p", "--port",
					dest="port",
					default="3306",
					action="store",)
	parser.add_option("-c", "--cmd",
					dest="cmd",
					default="check",
					action="store",)
	return parser.parse_args()

def _init():
	if not os.path.exists(MYSQL_DATA_DIR):
		os.makedirs(MYSQL_DATA_DIR)
	if not os.path.exists(MYSQL_CONF_DIR):
		os.makedirs(MYSQL_CONF_DIR)

def readConfs():
	import glob
	confs = glob.glob(os.path.join(MYSQL_CONF_DIR, '*.cnf'))
	return [MySQLDConfig(c) for c in confs]

def checkPort(d, p):
	for m in d:
		if p == m.mysqld_vars['port']:
			return True
	return False

def _genDict(name, port):
	return {
		'pid-file': os.path.join(MYSQL_DATA_DIR,name,"%s.pid" %name),
		'socket': "/tmp/%s.sock" % name,
		'port': port,
		'datadir': os.path.join(MYSQL_DATA_DIR, name),
		'log-error': os.path.join(MYSQL_DATA_DIR, name, "%s.log" % name)
		}

def mysql_install_db(cnf):
	cmd = ['mysql_install_db', '--defaults-file=%s' % cnf]
	p = Popen(cmd, stdout=PIPE)
	stdout, stderr = p.communicate()
	return p.returncode

def setOwner(datadir):
	os.system("chown -R mysql:mysql %s" % datadir)

def run_mysql(cnf):
	cmd = 'mysqld_safe --defaults-file=%s &' % cnf
	p = Popen(cmd, stdout=PIPE, shell=True)
	#stdout, stderr = p.communicate()
	time.sleep(5)
	return p.returncode

def createInstance(name, port, dbtype='master', **kw):
	cnf = os.path.join(MYSQL_CONF_DIR, "%s.cnf" % name)
	datadir = os.path.join(MYSQL_DATA_DIR, name)
	exists_confs = readConfs()
	if checkPort(exists_confs, port):
		print >> sys.stderr, "Port exists"
		sys.exit(-1)
	if not os.path.exists(cnf):
		c = _genDict(name, port)
		c.update(kw)
		mc = MySQLDConfig(cnf, **c)
		mc.save()
	else:
		mc = MySQLDConfig(cnf)
	if not os.path.exists(datadir):
		mysql_install_db(cnf)
		setOwner(datadir)
		run_mysql(cnf)	
		cur = connMySQL(name)
		setReplMaster(cur)
def statusMySQL(cnf):
	s = MySQLDConfig(cnf).mysqld_vars['socket']
	if os.path.exists(s):
		print '\033[40;36m' + 'The status is running' + '\033[0m'
		return True
	else:
		print '\033[40;36m' + 'The status is not running' + '\033[0m'
		return False

def startMySQL(cnf,datadir):
	if not statusMySQL(cnf):
		if os.path.exists(datadir) and os.path.exists(cnf):
			setOwner(datadir)
			run_mysql(cnf)
			s = 'Start the Instance ...OK'
		else:
			s = 'The instance is not exists,please create first'
	#else:
	#	s = 'The Instance\'s status is running'
		print '\033[40;33m' + s + '\033[0m'

def stopMySQL(cnf):
	if statusMySQL(cnf):
		s = MySQLDConfig(cnf).mysqld_vars['socket']
		cmd = 'mysqladmin shutdown -S %s' % s
		p = Popen(cmd,stdout=PIPE,shell=True)
		s = 'Stop the Instance ...succeed'
	#else:
	#	s = 'The Instance\'s status is not running'
		print '\033[40;33m' + s + '\033[0m'

def setReplMaster(cur):
	sql = "grant replication slave on *.* to %s@'%%' identified by '%s'" % (REPLICATION_USER, REPLICATION_PASS)
	cur.execute(sql)
	
def getCNF(name):
	return os.path.join(MYSQL_CONF_DIR,"%s.cnf" % name)

def connMySQL(name):
	cnf = getCNF(name)
	if os.path.exists(cnf):
		mc = MySQLDConfig(cnf)
		host = '127.0.0.1'
		port = int(mc.mysqld_vars['port'])
		user = 'root'
		conn = MySQLdb.connect(host=host, port=port, user=user)
		cur = conn.cursor()
		return cur
	

def diffVariables(name):
	cnf = getCNF(name)
	cur = connMySQL(name)
	vars = getMyVariables(cur)
	if os.path.exists(cnf):
		mc = MySQLDConfig(cnf)
		for k, v in mc.mysqld_vars.items():
			k = k.replace('-','_')
			if k in vars and vars[k] != v:
				print k, v, vars[k]

def setVariable(name, variable, value):
	cnf = getCNF(name)
	if os.path.exists(cnf):
		mc = MySQLDConfig(cnf)
		mc.set_var(variable, value)
		mc.save()

def findLogPos(s):
		import re
		rlog = re.compile(r"MASTER_LOG_FILE='(\S+)',",re.I)
		rpos = re.compile(r"MASTER_LOG_POS=(\d+);$",re.I)
		log = rlog.search(s)
		pos = rpos.search(s)
		if log and pos:
			return log.group(1), int(pos.group(1))
		else:
			return (None,None)
def getLogPos(f):
	with open(f) as fd:
		for l in fd:
			f,p = findLogPos(l)
			if f and p:
				return f,p
		
def changeMaster(cur, host,port,user,password,log_file=None,log_pos=None):
	sql = """change master to 
			master_host='%s',
			master_port=%s,
			master_user='%s',
			master_password='%s'""" % (host,port,user,password)
	if log_file:
		sql += """,master_log_file='%s',master_log_pos=%s""" % (log_file,log_pos)
		
	print sql
	cur.execute(sql)

def backupMySQL(name):
	cnf = getCNF(name)
	if os.path.exists(cnf):
		mc = MySQLDConfig(cnf)
		import datetime
		now = datetime.datetime.now()
		timestamp = now.strftime("%Y-%m-%d.%H.%M.%S")
		backup_file = os.path.join(MYSQL_BACK_DIR,name,timestamp+'.sql')
		_dir = os.path.dirname(backup_file)
		if not os.path.exists(_dir):
			os.makedirs(_dir)
		cmd = "mysqldump -A -x -F --master-data=1 --host=127.0.0.1 --user=root --port=%s > %s" % (mc.mysqld_vars['port'], backup_file)
		runMySQLCmd(cmd)

def runMySQLCmd(cmd):
	p = Popen(cmd, stdout=PIPE, shell=True)
	stdou, stderr = p.communicate()
	return p.returncode

def restoreMySQL(name,port,sqlfile,**kw):
	createInstance(name, port,'slave',**kw)
	cnf = getCNF(name)
	if os.path.exists(cnf):
		mc = MySQLDConfig(cnf)
		cmd = 'mysql --host 127.0.0.1 --user root --port %s < %s' % (mc.mysqld_vars['port'],sqlfile)
		runMySQLCmd(cmd)

def main():
	_init()
	opt, args = opts()
	instance_name = opt.name
	instance_port = opt.port
	command = opt.cmd
	print '\033[40;31m' + 'The Instance name is %s, And the Port is %s' % (instance_name,instance_port) + '\033[0m'
	cnf = os.path.join(MYSQL_CONF_DIR, "%s.cnf" % instance_name)
	datadir = os.path.join(MYSQL_DATA_DIR, instance_name)


	if command == 'create':
		if not args:		
			createInstance(instance_name, instance_port)
		else:
			dbtype = args[0]
			serverid = args[1]
			mysqld_options = {'server-id':serverid}
			if dbtype == 'master':
				mysqld_options['log-bin'] = 'mysql-bin'	
				createInstance(instance_name,
							instance_port,
							dbtype,
							**mysqld_options)
			elif dbtype == 'slave':
				host = args[2]
				port = args[3]
				user = REPLICATION_USER
				password = REPLICATION_PASS
				#mysqld_options['master-host'] = master_host
				#mysqld_options['master-port'] = master_port
				#mysqld_options['master-user'] = REPLICATION_USER
				#mysqld_options['master-password'] = REPLICATION_PASS
				mysqld_options['replicate-ignore-db'] = 'mysql'
				mysqld_options['skip-slave-start'] = None
				createInstance(instance_name,
							instance_port,
							dbtype,
							**mysqld_options)
				cur = connMySQL(instance_name)
				changeMaster(cur,host,port,user,password)
	elif command == 'start':
		startMySQL(cnf,datadir)
	elif command == 'status':
		statusMySQL(cnf)
	elif command == 'stop':
		stopMySQL(cnf)
	elif command == 'check':
		diffVariables(instance_name)	
	elif command == 'adjust':
		variable = args[0]
		value = args[1]
		setVariable(instance_name, variable, value)
	elif command == 'backup':
		backupMySQL(instance_name)
	elif command == 'restore':
		serverid = args[0]
		master_host = args[1]
		master_port = args[2]
		master_user = REPLICATION_USER
		master_password = REPLICATION_PASS
		sqlfile = args[3]
		log_file, log_pos = getLogPos(sqlfile)
		mysqld_options = {'server-id': serverid}
		mysqld_options['replicate-ignore-db'] = 'mysql'
		mysqld_options['skip-slave-start'] = None
		restoreMySQL(instance_name, instance_port, sqlfile, **mysqld_options)
		cur = connMySQL(instance_name)
		changeMaster(cur,master_host,master_port,master_user,master_password,log_file,log_pos)
if __name__ == '__main__':
	main()





