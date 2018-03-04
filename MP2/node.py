import socket
import hashlib
import copy
from threading import Thread, Lock # Import thread library
from time import sleep, time



M = 10				#M bits for chord alg
curr_file = None	#file to write to in batch call
curr_command = 0	#number of commands processed so far
batch_size = 0		#number of commands in batch file
heartbeats = [-1] * 10	
batch_array = None	#stores return valeus until written to file

#class used to store all node info 
class Node:
	vm = -1
	key = -1
	loc = None 

#hashes input
def findHash(s):
	hash_str = hashlib.sha1(s).hexdigest()
	hash_val = int(hash_str,16) & 0x3FF
	return hash_val


#prints finger table, used for debugging
def printTable():
	#print "Table for vm-" + str(host_num) + " key: " + str(my_vm)
	for i in range(0,M):
		x = ''
		if fingerTable[i] == None:
			x = "N/A"
		else:
			x = str(fingerTable[i].key)
		#print "Entry at i = " + str(i) + ": " + x

#checks if new connection is better pred
def updatePred(conn_idx):
	global pred
	new_key = outgoing_c[conn_idx].key
	if pred is None:
		pred = outgoing_c[conn_idx]
	if pred.key > my_vm:
		if new_key > pred.key:
			pred = outgoing_c[conn_idx]
		elif new_key < my_vm:
			pred = outgoing_c[conn_idx]
	else:
		if new_key < my_vm and new_key > pred.key:
			pred = outgoing_c[conn_idx]
	#print "Pred is: " + str(pred.key)

#checks where connection fits in finger table	
def updateFingertable(conn_idx):
	global fingerTable
	curNode = outgoing_c[conn_idx]
	curr_key = curNode.key
	if curr_key < my_vm:
		curr_key = curr_key + 2**M
	for i in range(0,M):
		finger_val = my_vm + 2**i
		if curr_key >= finger_val:
			# check original node_key val in ft[i]
			if fingerTable[i] == None:
				fingerTable[i] = curNode
				continue
			test_key = fingerTable[i].key
			if test_key < finger_val:
				test_key = test_key + 2**M
			if curr_key < test_key:
				fingerTable[i] = curNode
	#printTable()

#processes user input and branches to function to handle request
def process_instruction(instruction, batch):
	space1 = instruction.find(' ')
	space2 = instruction.find(' ', space1+1)
	cmd = instruction[0:space1]
	if(instruction == "LIST_LOCAL"):
		list_local(batch)	
	elif(cmd == "SET"):
		arg1 = instruction[space1+1:space2]
		#if arg1[0].isalpha() is False:
		#	print "Key must begin with alphabetic character"
		#	return
		arg2 = instruction[space2+1:]
		set_key(host_num, arg1, arg2, 0, batch)
	elif(cmd == "GET"):
		arg1 = instruction[space1+1:]
		#if arg1[0].isalpha() is False:
		#	print "Key must begin with alphabetic character"
		#	return
		get_key(host_num, arg1, 0, batch)
	elif(cmd == "OWNERS"):
		arg1 = instruction[space1+1:]
		#if arg1[0].isalpha() is False:
		#	print "Key must begin with alphabetic character"
		#	return
		get_owners(host_num, arg1, 0, batch)
	elif(cmd == "BATCH"):
		arg1 = instruction[space1+1:space2]
		arg2 = instruction[space2+1:]
		t_beginBatch = Thread(target = batch_cmd, args = (arg1,arg2, ))
		t_beginBatch.start()
	else:
		print "Invalid command"
	
# server functionality for accepting connections from clients
def server(s):
	global incoming_c
	global heartbeats
	while True:
		c, addr = s.accept()
		node_name = socket.gethostbyaddr(addr[0])[0]
		node_num = int(node_name[16])
		if node_num == 0:
			node_num = 10
		incoming_c[node_num-1] = c
		t = Thread(target = read_msg, args = (c,))
		t.start()
		t_sendhb = Thread(target = send_hb, args = (node_num-1,))
		t_sendhb.start()
		#print ("Accepted connection from node: " + str(node_num))
		if outgoing_c[node_num-1] == None:
			attempt_connection(node_num)
		heartbeats[node_num-1] = time()

#tries to connect to vm-i
def attempt_connection(i):
	global outgoing_c
	client_s = socket.socket()
	client_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	if i == 10:
		host = "sp17-cs425-g11-10.cs.illinois.edu"
	else:
		host = "sp17-cs425-g11-0"+str(i)+".cs.illinois.edu"
	try:
		client_s.connect((host, port))
		print ("Connected to node: " + str(i))
		new_node = Node()
		new_node.vm = i
		new_node.key = findHash("vm-" + str(i))
		new_node.loc = client_s
		outgoing_c[i-1] = new_node
		t = Thread(target = updateNewNode, args = (i-1,))
		t.start()
		t_heart = Thread(target = recv_hb, args = (i-1,))
		t_heart.start()
	except socket.error, e:
		a = 1

#reads in hbs and updates heartbeat array
def recv_hb(idx):
	global heartbeats
	global outgoing_c
	buf = ''
	if outgoing_c[idx] is None:
		return
	src = outgoing_c[idx].loc
	while True:
		try:
			# poll for data
			msg = src.recv(512)
		except socket.error, e:
			#print "recv error"
			return
		#print "read in msg"
		buf+=msg
		while buf:
			# check if this is a full message matching our protocol
			full_msg = buf.find('~')
			if full_msg == -1:
				break
			full_str = buf[0:full_msg]
			#print full_str
			space1 = full_str.find('`')
			space2 = full_str.find('`', space1+1)
			op = full_str[:space1]
			arg1 = full_str[space1+1:space2]
			if int(op) == 0:
				heartbeats[int(arg1) - 1] = time()
				#print "heartbeat from: " + arg1
			# check opcodes and jump to correct function
			#print "full message received: " + full_str
			
			buf = buf[full_msg+1:]

#checks if a key should be copied to new node
def shouldTransfer(unhashed_key):
	pred_key = pred.key
	key = findHash(unhashed_key)
	if pred_key < my_vm:
		if key <= pred_key or (key >= pred_key and key > my_vm):
			return True
	else:
		if key <= pred_key and key > my_vm:
			return True
	return False
		
	
#updates all local structures based off a new node
def updateNewNode(node_idx):
	global local_keys
	
	old_pred = copy.copy(pred)
	old_succ = copy.copy(fingerTable[0])
	updatePred(node_idx)
	updateFingertable(node_idx)
	if old_pred == None or old_succ == None:
		return


	if old_pred.vm != pred.vm:
		msg = encode_msg(6,host_num,0,0,0, 0)
		send_msg(msg, old_pred.loc)
		send_msg(msg, fingerTable[0].loc)

		for key in local_keys.keys():
			if shouldTransfer(key):
				value = local_keys[key][0]
				set_key(-1,key, value, 0, 0)
				del local_keys[key]

		for key,value in local_keys.iteritems():
			if value[1] == host_num:
				replicateKey(key, value[0])
		
	
	elif old_succ.vm != fingerTable[0].vm:
	
		msg = encode_msg(6,host_num,0,0,0, 0)
		send_msg(msg, old_succ.loc)
		for key,value in local_keys.iteritems():
			if value[1] == host_num:
				replicateKey(key, value[0])
		
		
		
#read in non-hb messages and process them
def read_msg(src):
	global local_keys
	# utilize a buffer to collect stream of network data collected from recv()
	buf = ''
	while True:
		try:
			# poll for data
			msg = src.recv(512)
		except socket.error, e:
			#print "read msg error"
		#TODO: Failure handling??
			return
		buf+=msg
		while buf:
			# check if this is a full message matching our protocol
			full_msg = buf.find('~')
			if full_msg == -1:
				break
			full_str = buf[0:full_msg]
			# check opcodes and jump to correct function
			#print "full message received: " + full_str
			t_proc = Thread(target=process_msg,args=(full_str,))
			t_proc.start()		
			#process_msg(full_str)
			buf = buf[full_msg+1:]


#Processes messages from other nodes
def process_msg(full_str):
	(op, arg1, arg2, arg3, arg4, arg5) = decode_msg(full_str)
	#if op !=0:
		#print "args: " + " " + str(op) + " " + str(arg1) + " " + str(arg2) + " " + str(arg3) + " " + str(arg4) + " " + str(arg5)
#	if op == 0:
#		heartbeats[int(arg1) - 1] = time()
		#print "HB received at: " + str(heartbeats[int(arg1) - 1])
	if op == 1:
		set_key(int(arg1), arg2, arg3, int(arg4), arg5)
	elif op ==2:
		safePrint("SET OK", arg5)
	elif op==3:
		get_key(int(arg1),arg2, int(arg4), arg5) 
	elif op==4:
		found = int(arg1)
		if found:
			msg = "Found: " + arg3 	
			safePrint(msg, arg5)	
		else:
			safePrint("Not found", arg5)
	elif op==5:
		local_keys[arg2] = (arg3, int(arg1))
	elif op==6:
		deleteReplicas(int(arg1))
	elif op==7:
		get_owners(int(arg1),arg2, int(arg4), arg5) 
	elif op==8:
		found = int(arg1)
		if found:
			safePrint(arg3, arg5)	
		else:
			safePrint("Not found", arg5)
		
#checks if print should go to screen or batch file
def safePrint(msg, batch):
	global curr_command
	if batch==0:
		print msg
	else:
		batch_array[batch-1] = msg + "\n"
		curr_command = curr_command + 1
		#print str(curr_command)
		if curr_command > batch_size:
			t_buf = Thread(target = printToFile)
			t_buf.start()


#writes full batch output to file
def printToFile():
	global batch_array
	f = open(curr_file,'a')
	for line in batch_array:
		f.write(line)
	f.close()
	print "BATCH complete"

#Simply sends msg to desired location
def send_msg(msg, c):
	#print "About to send msg: " + msg
	try:
		c.sendall(msg)
		#if int(msg[0]) != 0:
		#	print msg
	except socket.error, e:
		i = 1
		#print "send_msg error"

#sends request to replicate key to pred and successor
def replicateKey(unhashed_key, value):
	msg = encode_msg(5, host_num, unhashed_key, value, "garbage", 0)
	send_msg(msg, pred.loc)
	send_msg(msg, fingerTable[0].loc)

#tells vm to delete all replicas from node
def deleteReplicas(vm_id):
	global local_keys
	for key in local_keys.keys():
		if local_keys[key][1] == vm_id:
			del local_keys[key]

#Find next node to route requests to using finger table
def find_next_node(key):
	temp = key
	returnNode = -1
	if key < my_vm:
		temp = temp + 2**M
	for i in range(M-1,-1,-1):
		if fingerTable[i] is None:
			continue
		finger_value = fingerTable[i].key
		if finger_value < my_vm:
			finger_value = finger_value + 2**M
		if finger_value <= temp:		# TODO:Set myself to 1 in call?
			returnNode = i	
			break
	return returnNode

#User command. Prints all local keys	
def	list_local(batch):
	if batch == 0:
		for key in sorted(local_keys.iterkeys()):
			#TODO: Remove hash portion for demo
			#print "Name: " + key + "  hash: " + str(findHash(key))
			print key
		print "END LIST"
	else:
		local_buf = ''
		for key in sorted(local_keys.iterkeys()):
			local_buf += key + "\n"
		local_buf+= "END LIST"
		safePrint(local_buf,batch)

#User command. Sets/updates key value pair
def set_key(req_vm, unhashed_key, value, final_dest, batch):
	global local_keys
	key = findHash(unhashed_key)
	#print "key: " + str(key)
	if final_dest:
			local_keys[unhashed_key] = (value, host_num)
			# message back host VM
			if req_vm != host_num:
				#print "sending msg to: " + str(outgoing_c[req_vm-1].key)
				if req_vm != -1:
					send_msg(encode_msg(2,req_vm,7,7,7, batch), outgoing_c[req_vm-1].loc)
				replicateKey(unhashed_key, value)
			else:
				replicateKey(unhashed_key, value)
				if req_vm != -1:
					safePrint("SET OK", batch)	
	else:
		finger_idx = find_next_node(key)
		if finger_idx == -1:
			if key == my_vm:
				local_keys[unhashed_key] = (value, host_num)
				if req_vm != host_num:
					#print "sending msg to: " + str(outgoing_c[req_vm-1].key)
					if req_vm != -1:
						send_msg(encode_msg(2,req_vm,7,7,7, batch), outgoing_c[req_vm-1].loc)
					replicateKey(unhashed_key, value)
				else:
					replicateKey(unhashed_key, value)
					if req_vm != -1:
						safePrint("SET OK", batch)	
			else:
				# forward to successor with final_flag
				# print "sending msg to successor: " + str(fingerTable[0].key) 
				send_msg(encode_msg(1,req_vm,unhashed_key,value,1, batch), fingerTable[0].loc)
		else:
			# call normal set on finger_idx in table
			send_msg(encode_msg(1,req_vm,unhashed_key,value,0, batch), fingerTable[finger_idx].loc)

#retrieves value of key
def get_key(req_vm, unhashed_key, final_dest, batch):
	key = findHash(unhashed_key)
	if final_dest:
			if unhashed_key in local_keys:
				# return value to owner
				if req_vm != host_num:
					send_msg(encode_msg(4,1, "garbage", local_keys[unhashed_key][0],"garbage", batch), outgoing_c[req_vm-1].loc)
				else:
					msg = "Found: " + local_keys[unhashed_key][0]
					safePrint(msg, batch)	
			else:
				# return no to owner
				if req_vm != host_num:
					send_msg(encode_msg(4,0, "garbage", "garbage","garbage", batch), outgoing_c[req_vm-1].loc)
				else:
					msg = "Not found"
					safePrint(msg, batch)

	else:
		finger_idx = find_next_node(key)
		if finger_idx == -1:
			if unhashed_key in local_keys:
				# return value to owner
				if req_vm != host_num:
					send_msg(encode_msg(4,1, "garbage", local_keys[unhashed_key][0],"garbage", batch), outgoing_c[req_vm-1].loc)
				else:
					msg= "Found: " + local_keys[unhashed_key][0]
					safePrint(msg,batch)

			else:
				# forward to successor with final_flag 
				send_msg(encode_msg(3,req_vm, unhashed_key,"garbage", 1, batch), fingerTable[0].loc)
		else:
			# call normal get on finger_idx in table
			send_msg(encode_msg(3,req_vm, unhashed_key,"garbage", 0, batch), fingerTable[finger_idx].loc)


#finds owners of a key
def get_owners(req_vm, unhashed_key, final_dest, batch):
	key = findHash(unhashed_key)
	if final_dest:
			if unhashed_key in local_keys:
				# return value to owner
				owners = getOwnersMsg()
				if req_vm != host_num:
					send_msg(encode_msg(8,1, "garbage", owners,"garbage", batch), outgoing_c[req_vm-1].loc)
				else:
					safePrint(owners, batch)	
			else:
				# return no to owner
				if req_vm != host_num:
					send_msg(encode_msg(8,0, "garbage", "garbage","garbage", batch), outgoing_c[req_vm-1].loc)
				else:
					msg = "Not found"
					safePrint(msg, batch)

	else:
		finger_idx = find_next_node(key)
		if finger_idx == -1:
			if key == my_vm:
				# return value to owner
				owners = getOwnersMsg()
				if req_vm != host_num:
					send_msg(encode_msg(8,1, "garbage", owners,"garbage", batch), outgoing_c[req_vm-1].loc)
				else:
					safePrint(owners,batch)

			else:
				# forward to successor with final_flag 
				send_msg(encode_msg(7,req_vm, unhashed_key,"garbage", 1, batch), fingerTable[0].loc)
		else:
			# call normal get on finger_idx in table
			send_msg(encode_msg(7,req_vm, unhashed_key,"garbage", 0, batch), fingerTable[finger_idx].loc)

#gets proper formatting for Owners request return msg
def getOwnersMsg():
	msg = ''
	if host_num == 10:
		msg+= "10 "
	else:
		msg+= "0" + str(host_num) + " "
	if pred.vm == 10:
		msg+= "10 "
	else:
		msg+= "0" + str(pred.vm) + " "
	if fingerTable[0].vm == 10:
		msg+= "10"
	else:
		msg+= "0" + str(fingerTable[0].vm)
	return msg

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
	return (int(arg1), arg2, arg3, arg4, arg5, int(arg6))
	
#executes batch command
def batch_cmd(file1, file2):
	global batch_array
	global curr_file
	global curr_command
	global batch_size
	in_file = open(file1, "r")
	lines = in_file.readlines()
	in_file.close()
	#curr_file = open(file2,'w')
	curr_file = file2
	batch_size = len(lines)	
	
	batch_array = [None] * batch_size
	curr_command = 1
	for i, line in enumerate(lines):
		line = line[:-1]
		process_instruction(line, i+1)
	
#rebalances keys after failure
def failRebalance(failed_id):
	global local_keys

	for key in local_keys.keys():
		value = local_keys[key][0]
		value_id = local_keys[key][1]
		if value_id == host_num:
			replicateKey(key, value)
		elif value_id == failed_id:
			set_key(-1,key, value, 0, 0)
			del local_keys[key]
			
# checks for failures by comparing current time to time of last hb received. Must be within our timeout time
def check_hb():
	global heartbeats
	global outgoing_c
	global incoming_c

	while True:
		for i, hb in enumerate(heartbeats):
			if hb == -1:
				continue
			local = time()
			if local - hb > 12:
				print "vm-" + str(i+1) + " has failed"
				#print "Last HB at: " + str(hb) + ", Current time: " + str(local)
				neighbor_flag = False
				if pred == outgoing_c[i]:
					neighbor_flag = True
				if fingerTable[0] == outgoing_c[i]:
					neighbor_flag = True
				heartbeats[i] = -1
				outgoing_c[i] = None
				incoming_c[i] = None

				t_fail = Thread(target = handle_failure, args = (i,neighbor_flag,))
				t_fail.start()

#updates finger tables and calls rebalancing if needed
def handle_failure(conn_idx, neighbor_flag):
	removeFingerTableEntry(findHash("vm-" + str(conn_idx+1)))
	if neighbor_flag:
		failRebalance(conn_idx+1)

#sends hb messages to a node
def send_hb(idx):
	msg = encode_msg(0, host_num, "0", "0", "0", 0)
	while True:
		c = incoming_c[idx]
		if c is None:
			return
		send_msg(msg, c)
		sleep(0.25)

#removes references to failured node and refills entries
def removeFingerTableEntry(key):
	global pred
	global fingerTable
	for i,entry in enumerate(fingerTable):
		if entry.key == key:
			fingerTable[i] = None
	if pred.key == key:
		pred = None
	for i in range(0,10):
		if outgoing_c[i] != None:
			updateFingertable(i)
			updatePred(i)

# socket initialization
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = socket.gethostname()
host_num = int(host[16])
if host_num ==0:
	host_num = 10
outgoing_c = [None] * 10
incoming_c= [None] * 10
fingerTable = [None] * M
pred = None
local_keys = dict()
my_vm = findHash("vm-" + str(host_num))
print "My key is: " + str(my_vm)
#outgoing_c[host_num-1] = True
port = 10001
s.bind((host, port))
s.listen(10)

# create thread to act as server
t_server = Thread(target = server, args = (s,))
t_server.start()
sleep(4 + 0.5 * host_num)

print "Initiating connections to servers ... "
for i in range (1, 11):
#TODO: HANDLE 10 NODES
	if i == host_num:
		continue
	if outgoing_c[i-1] == None:
		attempt_connection(i)	
print "Finished attempts to make connections ... "
print "\n-------------- KEY VALUE STORAGE SYSTEM --------------\n" 
print "--------My key is: " + str(my_vm) + "--------My node is: " + str(host_num) + "--------\n"


# start thread for sending heartbeats
#t_send_hb = Thread(target = send_hb)
#t_send_hb.start()

# start thread for checking heartbeats
t_check_hb = Thread(target = check_hb)
t_check_hb.start()

while True:
	user_cmd = raw_input("")
	process_instruction(user_cmd, 0)




