#!/usr/bin/env python3
# -*- coding: utf_8 -*-

'''
Before starting this script run the following steps:
1. Install:
	apt install mariadb-server mariadb-client libmariadb-dev
	pip3 install psutil mysqlclient sqlalchemy
2. Create user and DB in MySQL
	CREATE USER 'editor'@'localhost' IDENTIFIED BY '<password>';
	GRANT ALL PRIVILEGES ON telemetry.* TO 'editor'@'localhost';
	CREATE DATABASE telemetry;
3.
	CREATE USER 'frontend'@'localhost' IDENTIFIED BY '<password>';
	GRANT ALL PRIVILEGES ON frontend.* TO 'frontend'@'localhost';
	GRANT SELECT, SHOW VIEW ON telemetry.* TO 'frontend'@'localhost';
	CREATE DATABASE frontend;
'''


import sys
import time
import json
import urllib.request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import datetime as DateTimeLibrary
from models import Base, Data, Validator

sys.path.append("/usr/src/mytonctrl/")
from mypylib.mypylib import MyPyClass, Sleep
from mytoncore import MyTonCore


# Global vars
local = MyPyClass(__file__)
mainnet_ton = MyTonCore()
testnet_ton = MyTonCore()


def create_db_connect():
	global localdb
	local.AddLog("start create_db_connect function", "debug")
	# Create memory connect
	mysql = local.buffer.get("mysql")
	user = mysql.get("user")
	passwd = mysql.get("passwd")
	host = mysql.get("host")
	db = mysql.get("db")
	mysqlConnectUrl = f"mysql://{user}:{passwd}@{host}/{db}"
	engine = create_engine(mysqlConnectUrl, echo=False)
	Session = sessionmaker(bind=engine)
	session = Session()
	return engine, session
#end define

def close_db_connect(engine, session):
	local.AddLog("start close_db_connect function", "debug")
	session.commit()
	session.close()
	engine.dispose()
#end define

def clear_table(engine, session, table_class, save_coun, save_len=100000):
	table = table_class.__tablename__
	query = session.query(table_class)
	query_len = query.count()
	if query_len < save_coun*save_len:
		return
	query = query.order_by(table_class.id.desc())
	data = query.first()
	if data == None:
		return
	start = data.id

	k = start
	sql = "create table {table}_tmp like {table}".format(table=table)
	result = engine.execute(sql)

	for i in range(save_coun):
		sql = f"INSERT INTO {table}_tmp SELECT * FROM {table} WHERE id BETWEEN {start} - {save_len-1} AND {start}"
		result = engine.execute(sql)
		start -= save_len
	#end for

	# скопировать хвостик
	sql = f"INSERT INTO {table}_tmp SELECT * FROM {table} WHERE id > {k}"
	result = engine.execute(sql)
	session.commit()

	# Переименовать старую талибцу
	sql = f"ALTER TABLE {table} RENAME TO {table}_old"
	result = engine.execute(sql)
	session.commit()

	# Переименовать новую таблицу
	sql = f"ALTER TABLE {table}_tmp RENAME TO {table}"
	result = engine.execute(sql)
	session.commit()

	# Удалить старую таблицу
	sql = "DROP TABLE {table}_old".format(table=table)
	result = engine.execute(sql)
	session.commit()
#end define

def get_toncenter_data():
	local.AddLog("start get_toncenter_data function", "debug")
	timestamp = int(time.time())

	# Get gata
	toncenter = local.buffer.get("toncenter")
	api_key = toncenter.get("api_key")
	telemetry_url = f"https://telemetry.toncenter.com/getTelemetryData?timestamp_from={timestamp-100}&api_key={api_key}"
	text = try_get_url(telemetry_url)
	nodes = json.loads(text)

	return nodes
#end define

def try_get_url(url):
	for i in range(3):
		try:
			data = get_url(url)
			return data
		except Exception as err:
			time.sleep(1)
	raise Exception(f"try_get_url error: {err}")
#end define

def get_url(url):
	local.AddLog("start get_url function", "debug")
	req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
	web = urllib.request.urlopen(req, timeout=3)
	data = web.read()
	text = data.decode()
	return text
#end define

def init():
	# Set local config
	local.db["config"]["logLevel"] = "debug"
	local.Run()
	
	# Прописать в автозагрузку
	if "--add2systemd" in sys.argv:
		ux = sys.argv.index("-u")
		user = sys.argv[ux+1]
		start = "/usr/bin/python3 " + local.buffer.get("myPath")
		Add2Systemd(name="vload", user=user, start=start)
	#end if

	# Load settings
	filepath = local.buffer.get("myDir") + "settings.json"
	file = open(filepath, "rt")
	text = file.read()
	file.close()
	buff = json.loads(text)
	local.buffer.update(buff)
	
	# set TON configs
	mainnet_ton.liteClient.configPath = "/usr/bin/ton/global.config.json"
	testnet_ton.liteClient.configPath = "/usr/bin/ton/testnet-global.config.json"

	# Create all tables
	engine, session = create_db_connect()
	Base.metadata.create_all(engine)
	close_db_connect(engine, session)

	# Start threads
	local.StartCycle(save_telemetry, sec=60)
	local.StartCycle(save_config34, sec=60)
	local.StartCycle(clear_tables, sec=21600)
#end define

def save_telemetry():
	local.AddLog("start save_telemetry function", "debug")
	engine, session = create_db_connect()
	nodes = get_toncenter_data()
	mainnet_validators = local.TryFunction(mainnet_ton.GetValidatorsList)
	testnet_validators = local.TryFunction(testnet_ton.GetValidatorsList)
	print(f"save_telemetry testnet_validators: {len(testnet_validators)}")
	for node in nodes:
		adnl_address = node.get("adnl_address")
		mainnet_validator = list2dict(mainnet_validators).get(adnl_address)
		testnet_validator = list2dict(testnet_validators).get(adnl_address)
		if adnl_address == "null":
			continue
		save_node_data(node, session, mainnet_validator, testnet_validator)
	close_db_connect(engine, session)
#end define

def save_node_data(node, session, mainnet_validator, testnet_validator):
	datetime = DateTimeLibrary.datetime.now()
	adnl_address = node.get("adnl_address")
	data = node.get("data")
	remote_country = node.get("remote_country")
	remote_isp = node.get("remote_isp")
	
	# Get node data
	cpu_number = data.get("cpuNumber")
	db_usage = data.get("dbUsage")
	stake = data.get("stake")
	
	git_hashes = data.get("gitHashes")
	memory = data.get("memory")
	swap = data.get("swap")
	uname = data.get("uname")
	vprocess = data.get("vprocess")
	validator_status = data.get("validatorStatus")
	
	tps = get_first(data.get("tps"))
	cpu_load = get_first(data.get("cpuLoad"))
	net_load = get_first(data.get("netLoad"))
	pps = get_first(data.get("pps"))
	
	disks_load = data.get("disksLoad")
	disks_load_percent = data.get("disksLoadPercent")
	iops = data.get("iops")
	for disk_name in disks_load:
		disk_load = disks_load.get(disk_name)
		disk_load_percent = disks_load_percent.get(disk_name)
		disk_iops = iops.get(disk_name)
		disks_load[disk_name] = get_first(disk_load)
		disks_load_percent[disk_name] = get_first(disk_load_percent)
		iops[disk_name] = get_first(disk_iops)
	#end if
	
	# Get git_hashes data
	mytonctrl_hash = None
	validator_hash = None
	if git_hashes != None:
		mytonctrl_hash = git_hashes.get("mytonctrl")
		validator_hash = git_hashes.get("validator")
	#end if
	
	# Get memory data
	memory_total = None
	memory_usage = None
	if memory != None:
		memory_total = memory.get("total")
		memory_usage = memory.get("usage")
	#end if
	
	# Get swap data
	swap_total = None
	swap_usage = None
	if swap != None:
		swap_total = swap.get("total")
		swap_usage = swap.get("usage")
	#end if
	
	# Get uname data
	uname_machine = None
	uname_release = None
	uname_sysname = None
	uname_version = None
	if uname != None:
		uname_machine = uname.get("machine")
		uname_release = uname.get("release")
		uname_sysname = uname.get("sysname")
		uname_version = uname.get("version")
	#end if
	
	# Get vprocess data
	vprocess_cpu_percent = None
	vprocess_memory_data = None
	vprocess_memory_dirty = None
	vprocess_memory_lib = None
	vprocess_memory_rss = None
	vprocess_memory_shared = None
	vprocess_memory_text = None
	vprocess_memory_vms = None
	if vprocess != None:
		vprocess_cpu_percent = vprocess.get("cpuPercent")
		vprocess_memory = vprocess.get("memory")
		if vprocess_memory != None:
			vprocess_memory_data = vprocess_memory.get("data")
			vprocess_memory_dirty = vprocess_memory.get("dirty")
			vprocess_memory_lib = vprocess_memory.get("lib")
			vprocess_memory_rss = vprocess_memory.get("rss")
			vprocess_memory_shared = vprocess_memory.get("shared")
			vprocess_memory_text = vprocess_memory.get("text")
			vprocess_memory_vms = vprocess_memory.get("vms")
	#end if
	
	# Get validator_status data
	unixtime = None
	is_working = None
	out_of_sync = None
	masterchainblock = None
	masterchainblocktime = None
	gcmasterchainblock = None
	keymasterchainblock = None
	rotatemasterchainblock = None
	shardclientmasterchainseqno = None
	stateserializermasterchainseqno = None
	if validator_status != None:
		unixtime = validator_status.get("unixtime")
		is_working = validator_status.get("isWorking")
		out_of_sync = validator_status.get("outOfSync")
		masterchainblock = validator_status.get("masterchainblock")
		masterchainblocktime = validator_status.get("masterchainblocktime")
		gcmasterchainblock = validator_status.get("gcmasterchainblock")
		keymasterchainblock = validator_status.get("keymasterchainblock")
		rotatemasterchainblock = validator_status.get("rotatemasterchainblock")
		shardclientmasterchainseqno = validator_status.get("shardclientmasterchainseqno")
		stateserializermasterchainseqno = validator_status.get("stateserializermasterchainseqno")
	#end if
	
	# Get validator data
	validator = None
	network_name = None
	validator_pubkey = None
	validator_weight = None
	validator_mr = None
	validator_wr = None
	validator_efficiency = None
	validator_wallet_address = None
	if mainnet_validator != None:
		network_name = "mainnet"
		validator = mainnet_validator
	elif  testnet_validator != None:
		network_name = "testnet"
		validator = testnet_validator
	else:
		network_name = find_network_name_with_keymasterchainblock(session, keymasterchainblock)
	if validator != None:
		validator_pubkey = validator.get("pubkey")
		validator_weight = validator.get("weight")
		validator_mr = validator.get("mr")
		validator_wr = validator.get("wr")
		validator_efficiency = validator.get("efficiency")
		validator_wallet_address = validator.get("walletAddr")
	#end if
	
	# Create DB object
	data = Data(
		datetime = datetime,
		adnl_address = adnl_address,
		remote_country = remote_country,
		remote_isp = remote_isp,
		
		cpu_number = cpu_number,
		db_usage = db_usage,
		stake = stake,
		
		tps = tps,
		cpu_load = cpu_load,
		net_load = net_load,
		pps = pps,
		
		disks_load = json.dumps(disks_load),
		disks_load_percent = json.dumps(disks_load_percent),
		iops = json.dumps(iops),
		
		mytonctrl_hash = mytonctrl_hash,
		validator_hash = validator_hash,
		
		memory_total = memory_total,
		memory_usage = memory_usage,
		
		swap_total = swap_total,
		swap_usage = swap_usage,
		
		uname_machine = uname_machine,
		uname_release = uname_release,
		uname_sysname = uname_sysname,
		uname_version = uname_version,
		
		vprocess_cpu_percent = vprocess_cpu_percent,
		vprocess_memory_data = vprocess_memory_data,
		vprocess_memory_dirty = vprocess_memory_dirty,
		vprocess_memory_lib = vprocess_memory_lib,
		vprocess_memory_rss = vprocess_memory_rss,
		vprocess_memory_shared = vprocess_memory_shared,
		vprocess_memory_text = vprocess_memory_text,
		vprocess_memory_vms = vprocess_memory_vms,
		
		unixtime = unixtime,
		is_working = is_working,
		out_of_sync = out_of_sync,
		masterchainblock = masterchainblock,
		masterchainblocktime = masterchainblocktime,
		gcmasterchainblock = gcmasterchainblock,
		keymasterchainblock = keymasterchainblock,
		rotatemasterchainblock = rotatemasterchainblock,
		shardclientmasterchainseqno = shardclientmasterchainseqno,
		stateserializermasterchainseqno = stateserializermasterchainseqno,
		
		validator_pubkey = validator_pubkey,
		validator_weight = validator_weight,
		validator_mr = validator_mr,
		validator_wr = validator_wr,
		validator_efficiency = validator_efficiency,
		validator_wallet_address = validator_wallet_address,
		
		network_name = network_name
	)
	session.add(data)
#end define

def find_network_name_with_keymasterchainblock(session, keymasterchainblock):
	if keymasterchainblock is None:
		return
	query = session.query(Data)
	query = query.order_by(Data.id.desc())
	query = query.filter_by(keymasterchainblock=keymasterchainblock)
	data = query.first()
	if data is None:
		return
	return data.network_name
#end define

def save_config34():
	local.AddLog("start save_config34 function", "debug")
	engine, session = create_db_connect()
	mainnet_config34 = local.TryFunction(mainnet_ton.GetConfig34)
	testnet_config34 = local.TryFunction(testnet_ton.GetConfig34)
	mainnet_validators = mainnet_config34.get("validators")
	testnet_validators = testnet_config34.get("validators")
	print(f"save_config34 testnet_validators: {len(testnet_validators)}")
	for validator in mainnet_validators:
		adnl_address = validator.get("adnlAddr")
		if adnl_address == "null":
			continue
		save_validator_data(validator, session, config34=mainnet_config34, network_name="mainnet")
	for validator in testnet_validators:
		adnl_address = validator.get("adnlAddr")
		if adnl_address == "null":
			continue
		save_validator_data(validator, session, config34=testnet_config34, network_name="testnet")
	close_db_connect(engine, session)
#end define

def save_validator_data(validator, session, config34, network_name):
	start_work_time = config34.get("startWorkTime")
	end_work_time = config34.get("endWorkTime")
	total_weight = config34.get("totalWeight")
	
	datetime = DateTimeLibrary.datetime.now()
	adnl_address = validator.get("adnlAddr")
	validator_pubkey = validator.get("pubkey")
	validator_weight = validator.get("weight")
	weight = round(validator_weight / total_weight * 100, 2)
	
	query = session.query(Validator)
	query = query.order_by(Validator.id.desc())
	query = query.filter_by(start_work_time=start_work_time, validator_pubkey=validator_pubkey)
	data = query.first()
	if data != None:
		return
	#end if
	
	# Create DB object
	data = Validator(
		datetime = datetime,
		adnl_address = adnl_address,
		validator_pubkey = validator_pubkey,
		validator_weight = validator_weight,
		start_work_time = start_work_time,
		end_work_time = end_work_time,
		total_weight = total_weight,
		weight = weight,
		network_name = network_name
	)
	session.add(data)
#end define

def get_first(item, index=0):
	result = None
	try:
		if type(item) == list:
			result = item[index]
		elif type(item) == dict:
			buff = list(item.keys())[index]
			result = item.get(buff)
	except: pass
	return result
#end define

def list2dict(data):
	result = dict()
	if data is None:
		return result
	for item in data:
		adnl_address = item.get("adnlAddr")
		result[adnl_address] = item
	return result
#end define

def clear_tables():
	local.AddLog("start clear_tables function", "debug")
	engine, session = create_db_connect()
	clear_table(engine, session, Data, save_coun=50)
	clear_table(engine, session, Validator, save_coun=50)
	close_db_connect(engine, session)
#end define


###
### Start of the program
###

if __name__ == "__main__":
	init()
	Sleep()
#end if
