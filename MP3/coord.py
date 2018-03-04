import socket
#import copy
from threading import Thread, Lock # Import thread library
from time import sleep, time

# data structures for holding connections to clients and servers
clients = [None] * 3
servers = [None] * 5

# list for keeping track of acknowledgements seen per client
ack_counts = [0] * 3

# defines structure use in deadlock detection
waits = [[0 for x in range(3)] for y in range(3)]

# when a server attempts to wait for a lock or gets a lock, this method is called
# to set appropriate values in the waits 2d array
def updateWaits(client_id, waitsFor):
	global waits
	if waitsFor == 0:
		# clears values for this transaction waiting on others
		waits[client_id - 1][0] = 0
		waits[client_id - 1][1] = 0
		waits[client_id - 1][2] = 0
	else:
		# sets value indicating that this transaction is waiting on a lock owned by another
		waits[client_id - 1][waitsFor - 1] = 1

# method for checking all scenarios for the existence of a deadlock
def checkDeadlock():
	if waits[0][1] and waits[1][0]:
		sleep(1) 
		if waits[0][1] and waits[1][0]: # check again to eliminate chance of false positive
			print "DEADLOCK DETECTED: 1 2"
			msg = encode_msg(4,0,0,0,2,1) # client_id 2
			send_servers_msg(msg)

	if waits[0][2] and waits[2][0]:
		sleep(1)
		if waits[0][2] and waits[2][0]:
			print "DEADLOCK DETECTED: 1 3"
			msg = encode_msg(4,0,0,0,3,1) # client_id 3
			send_servers_msg(msg)

	if waits[1][2] and waits[2][1]:
		sleep(1)
		if waits[1][2] and waits[2][1]:
			print "DEADLOCK DETECTED: 2 3"
			msg = encode_msg(4,0,0,0,3,1) # client_id 3
			send_servers_msg(msg)

	if waits[0][1] and waits[1][2] and waits[2][0]:
		sleep(1)
		if waits[0][1] and waits[1][2] and waits[2][0]:
			print "DEADLOCK DETECTED: 1->2->3->1"
			msg = encode_msg(4,0,0,0,3,1) # client_id 3
			send_servers_msg(msg)

	if waits[0][2] and waits[2][1] and waits[1][0]:
		sleep(1)
		if waits[0][2] and waits[2][1] and waits[1][0]:
			print "DEADLOCK DETECTED: 1->3->2->1"
			msg = encode_msg(4,0,0,0,3,1) # client_id 3
			send_servers_msg(msg)

# sends a message to all servers in the system
def send_servers_msg(msg):
	global servers
	for server in servers:
		if server != None:
			try:
				server.sendall(msg)
			except socket.error, e:
				i = 1

# server functionality for accepting connections from clients
def server(s):
	global clients
	global servers
	while True:
		c, addr = s.accept()
		node_name = socket.gethostbyaddr(addr[0])[0]
		node_num = int(node_name[16])
		print "Received connection from " + str(node_num)
		
		if node_num <=3:
			# add new conneciton to global clients structure
			clients[node_num-1] = c
			t_read = Thread(target = read_msg, args = (c,True,))
			t_read.start()
		else:
			# add new connection to global servers structure
			servers[node_num-4] = c
			t_read = Thread(target = read_msg, args = (c,False,))
			t_read.start()

		
#read in non-hb messages and process them
def read_msg(src, is_client):
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
			#print "full message received: " + full_str
	
			# call correct method to process message depending on src = server/client connection
			if is_client:
				t_proc = Thread(target=process_msg_client,args=(full_str,))
				t_proc.start()		
			else:	
				t_proc = Thread(target=process_msg_server,args=(full_str,))
				t_proc.start()
			buf = buf[full_msg+1:]


# processes messages from other nodes
def process_msg_client(full_str):
	global ack_counts
	(op, server, arg2, arg3, client_id, arg5) = decode_msg(full_str)
	if op == 0:
		print "beginning transaction from client " + str(client_id)
		#forward to servers???
		ack_counts[client_id-1] = 0
	# forward message to servers
	elif op == 3 or op ==4:
		for item in servers:
			if item != None:
				send_msg(full_str + '~', item)
	else:
		send_msg(full_str + '~', servers[server])
		#print full_str

#Processes messages from other nodes
def process_msg_server(full_str):
	global ack_counts
	(op, server, arg2, arg3, client_id, trans_id) = decode_msg(full_str)
	#print full_str
	if op == 1:
		#send OK to client_id 
		send_msg("OK~", clients[client_id-1])
	elif op == 2:
		if arg3	== "NOT FOUND":
			#send not found to user
			send_msg("NOT FOUND~", clients[client_id-1])
			#tell all servers to abort that client
			msg = encode_msg(4, 0, 0, 0, client_id, 2)
			for item in servers:
				if item != None:
					send_msg(msg, item)
		else:
			msg = chr(server+ord('A')) + "." + arg2 + " = " + arg3 + '~'
			#send found: val to proper user
			send_msg(msg,clients[client_id-1])
			
	elif op == 3:
		ack_counts[client_id-1]+=1
		if ack_counts[client_id-1] == 5:
			#send COMMIT OK to user	
			send_msg("COMMIT OK~", clients[client_id-1])
	elif op == 4:
		ack_counts[client_id-1]+=1
		if ack_counts[client_id-1] == 5:
			#send ABORT to user	
			send_msg("ABORT~", clients[client_id-1])
	elif op == 5:
		updateWaits(client_id, trans_id)
		checkDeadlock()
		


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
	return (int(arg1), int(arg2), arg3, arg4, int(arg5), int(arg6))
	
# socket initialization
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = socket.gethostname()
host_num = int(host[16])
if host_num ==0:
	host_num = 10
port = 10001
s.bind((host, port))
s.listen(10)

# create thread to act as server
t_server = Thread(target = server, args = (s,))
t_server.start()
print "Waiting for connections from servers ... "
# TODO: Wait for servers?
print "Finished attempts to make connections ... "
print "\n-------------- COORDINATOR --------------\n" 



while True:
	a = 1	



