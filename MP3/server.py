import socket
from threading import Thread, Lock, Event # Import thread library
from time import sleep, time


storage = dict() #permanent storage for keys/values
locks = dict()   #mapping of key to RWLock for that key
locks_lock = Lock() #lock for editing the locks list
c1_flag = Event()	#flags used for signaling a new instruction to a thread processing a trnasaction
c2_flag = Event()
c3_flag = Event()
pending = [None] * 3 #stores the pending instructions
deadlock_flag = [False] * 3 #flag used to indicate a deadlock

#Read write lock class.
class RWLock(object):

	def __init__(self):
		self.read_lock = Lock()
		self.write_lock = Lock()
		self.owner = []
		self.count = 0

	def try_read(self,client_idx):
		flag = False
		if self.read_lock.acquire(False):
			if self.count == 0:
				if self.write_lock.acquire(False):
					self.owner.append(client_idx)
					self.count+=1
					flag = True
			else:
				flag = True
				self.count+=1
				self.owner.append(client_idx)
			self.read_lock.release()

		return flag
	
	def try_write(self,client_idx):
		if self.write_lock.acquire(False):
			self.owner = [client_idx]
			return True
		return False

	def release_read(self, client_idx):
		self.read_lock.acquire()
		self.count-=1
		self.owner.remove(client_idx)
		if self.count == 0:
			self.write_lock.release()
		self.read_lock.release()

	def release_write(self):
		self.owner = []
		self.write_lock.release()
		

	def try_promote(self, client_idx):
		flag = False
		if self.read_lock.acquire(False):
			if self.count == 1:
				self.count = 0
				self.owner = [client_idx]
				flag = True
			self.read_lock.release()
		return flag
		

	def get_owner(self):
		return self.owner



#These 3 functions are wrappers for the RWLock calls, which check for deadlocks.
def acquire_read_deadlock(key, client_idx):
	global deadlock_flag
	first_try = True	
	while True:
		#try lock
		if locks[key].try_read(client_idx):
			#send msg saying no deadlocks
			msg = encode_msg(5,host_num-4,0,"OK",client_idx+1,0)
			send_msg(msg)
			return True
		#if first attempt, tell coord that you're waiting on someone
		if first_try:
			first_try = False
			#send msg saying deadlock with owner
			cur_owners = locks[key].get_owner()
			for item in cur_owners:
				if item != client_idx:
					msg = encode_msg(5,host_num-4,0,"OK",client_idx+1,item+1)
					send_msg(msg)
		#flag set if coord decides to terminate this transaction from deadlock
		if deadlock_flag[client_idx]:
			deadlock_flag[client_idx] = False
			return False

def acquire_write_deadlock(key, client_idx):
	global deadlock_flag
	first_try = True	
	while True:
		if locks[key].try_write(client_idx):
			#send msg saying no deadlocks
			msg = encode_msg(5,host_num-4,0,"OK",client_idx+1,0)
			send_msg(msg)
			return True
		if first_try:
			first_try = False
			#send msg saying deadlock with owner
			cur_owners = locks[key].get_owner()
			for item in cur_owners:
				if item != client_idx:
					msg = encode_msg(5,host_num-4,0,"OK",client_idx+1,item+1)
					send_msg(msg)
		if deadlock_flag[client_idx]:
			deadlock_flag[client_idx] = False
			return False

def promote_deadlock(key, client_idx):
	global deadlock_flag
	first_try = True	
	while True:
		if locks[key].try_promote(client_idx):
			msg = encode_msg(5,host_num-4,0,"OK",client_idx+1,0)
			send_msg(msg)
			#send msg saying no deadlocks
			return True
		if first_try:
			first_try = False
			#send msg saying deadlock with owner
			cur_owners = locks[key].get_owner()
			for item in cur_owners:
				if item != client_idx:
					msg = encode_msg(5,host_num-4,0,"OK",client_idx+1,item+1)
					send_msg(msg)
		if deadlock_flag[client_idx]:
			deadlock_flag[client_idx] = False
			return False
	
#read in  messages and process them
def read_msg(src):
	# utilize a buffer to collect stream of network data collected from recv()
	#print "start of read_msg"
	buf = ''
	while True:
		try:
			# poll for data
			msg = src.recv(512)
		except socket.error, e:
			print "ERROR: Coord connection lost"
			return
		buf+=msg
		while buf:
			# check if this is a full message matching our protocol
			full_msg = buf.find('~')
			if full_msg == -1:
				break
			full_str = buf[0:full_msg]
			# Open new thread to process msg
			t_proc = Thread(target=process_msg,args=(full_str,))
			t_proc.start()		
			buf = buf[full_msg+1:]


#Processes messages from other nodes
def process_msg(full_str):
	global deadlock_flag
	(op, arg1, arg2, arg3, client_id, arg5) = decode_msg(full_str)
	#store instruction in pending spot for the proper thread
	pending[client_id-1] = full_str
	#this indicates a deadlock. set proper flag
	if arg5 ==1:
		deadlock_flag[client_id-1] = True	
	#notify proper thread using Events
	if client_id == 1:
		c1_flag.set()
	elif client_id ==2:
		c2_flag.set()
	elif client_id == 3:
		c3_flag.set()

	
#Simply sends msg to desired location
def send_msg(msg):
	try:
		s.sendall(msg)
	except socket.error, e:
		i = 1
		#print "send_msg error"

#encodes message args into our protocal style
def encode_msg(arg1, arg2, arg3, arg4, arg5, arg6):
	return str(arg1) + '`' +str(arg2) + '`' +str(arg3) + '`' +str(arg4) + '`' +str(arg5) + '`' +str(arg6) + '~'

#decodes message into specific args
def decode_msg(msg):
	space1 = msg.find('`')
	space2 = msg.find('`', space1+1)
	space3 = msg.find('`', space2+1)
	space4 = msg.find('`', space3+1)
	space5 = msg.find('`', space4+1)
	arg1 = msg[:space1]
	arg2 = msg[space1+1:space2]
	arg3 = msg[space2+1:space3]
	arg4 = msg[space3+1:space4]
	arg5 = msg[space4+1:space5]
	arg6 = msg[space5+1:]
	return (int(arg1), arg2, arg3, arg4, int(arg5), int(arg6))


#Handles all commands for specific client
def handle_transaction(client_idx, flag):
	global deadlock_flag
	global storage
	global locks
	global locks_lock
	#Local variables storing locks owned and pending changes
	locks_owned = dict()
	uncommitted_keys = dict()

	while True:
		#Wait on next instruction
		flag.wait()

		##################### PROCESS INSTRUCTION #################################	

		instruction = pending[client_idx] 
		(op, server_num, key, value, client_id, trans_id) = decode_msg(instruction)

		#SET
		if op==1:
			#first check storage for key
			if key in storage:
				if key in locks_owned:
					if locks_owned[key] == "r":
						#promote lock to write
						if promote_deadlock(key,client_idx) is False:
							continue
						locks_owned[key] ="w"
				else:
					if acquire_write_deadlock(key, client_idx) is False:
						continue
					locks_owned[key] = "w"
				uncommitted_keys[key] = value
			#Then check uncommitted keys
			elif key in uncommitted_keys:
				# we should already own the write lock
				uncommitted_keys[key] = value
			#Else must make key value pair, and potentially a new lock
			else:
				#Get lock to modify locks list
				locks_lock.acquire()
				if key not in locks:
					l = RWLock()
					locks[key] = l # create a lock for this new key
				if acquire_write_deadlock(key,client_idx) is False:
					continue
				locks_lock.release()
				locks_owned[key] = "w"
				uncommitted_keys[key] = value
			# send OK to coord
			msg = encode_msg(1,host_num-4,0,"OK",client_id,0)
			send_msg(msg)
		
		#GET	
		if op==2:
			#first check uncommitted keys for most updated value
			if key in uncommitted_keys:
				if key not in locks_owned:
					if acquire_read_deadlock(key, client_idx) is False:
						continue
					locks_owned[key] = "r"
				retval = uncommitted_keys[key]
			#Then check storage
			elif key in storage:
				if key not in locks_owned:
					if acquire_read_deadlock(key, client_idx) is False:
						continue
					locks_owned[key] = "r"
				retval = storage[key]
			
			else:
				retval = "NOT FOUND"
			# send to coord
			msg = encode_msg(2,host_num-4,key,retval,client_id,0)
			send_msg(msg)
		
		#COMMIT
		if op==3:
			#Commit all changes to permenant storage
			for key in uncommitted_keys:
				storage[key] = uncommitted_keys[key]
			#Unlock all keys
			for key in locks_owned:
				if locks_owned[key] == "r":
					locks[key].release_read(client_idx)
				else:
					locks[key].release_write()
			#clear local storage for next transaction
			locks_owned.clear()
			uncommitted_keys.clear()
			msg = encode_msg(3,host_num-4,key,"COMMIT OK",client_id,0)
			send_msg(msg)
			deadlock_flag[client_idx] = False

		#ABORT
		if op==4:
			#Unlock all keys
			for key in locks_owned:
				if locks_owned[key] == "r":
					locks[key].release_read(client_idx)
				else:
					locks[key].release_write()
			#Erase pending changes
			locks_owned.clear()
			uncommitted_keys.clear() 
			if trans_id!=2:
				msg = encode_msg(4,host_num-4,0,"ABORTED",client_id,0)
				send_msg(msg)
			deadlock_flag[client_idx] = False

		# instruction done
		flag.clear()








#----------------------------- MAIN ------------------------------------------------------	
# socket initialization
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = socket.gethostname()
host_num = int(host[16])
port = 10001

c1_flag.clear()
c2_flag.clear()
c3_flag.clear()

print "Initiating connection to coordinator ... "

coord_ip = "sp17-cs425-g11-09.cs.illinois.edu"

while True:
	try:
		s.connect((coord_ip, port))
		break
	except socket.error, e:
		a = 1

print "\n-------------- TRANSACTIONS --------------\n" 
print "--------My node is: " + str(host_num) + "--------\n"

t_read = Thread(target = read_msg, args = (s,))
t_read.start()

#Start 3 threads to process the 3 potential transactions from 3 clients
t_1 = Thread(target = handle_transaction, args = (0, c1_flag,))
t_1.start()
t_2 = Thread(target = handle_transaction, args = (1, c2_flag,))
t_2.start()
t_3 = Thread(target = handle_transaction, args = (2, c3_flag,))
t_3.start()


#Prints debugging information about server
while True:
	a = 1
	user_cmd = raw_input("")
	if user_cmd == "storage":
		for key in storage:
			print "key: " + str(key) + "  value: " + str(storage[key])
	elif user_cmd == "locks":
		for key in locks:
			print "lock for key: " + str(key)
	else:
		print "type a server command"




