import socket
import sys
import math
from threading import Thread, Lock # Import thread library
from time import sleep

# priority queue holding item objects
queue = []

# local sequence number 
seq = 0

# array holding maximum priority seen for a specific message sent by p
max_priority = [0] * 32

# array holding number of responses seen for a specific message sent by p
node_count = [0] * 32

# local counter for responses from other processes in chat system
response_count = 1

# holds values for other process's host number
client_idx = []
client_sockets_idx = []

# lock for accessing critical sections
fail_lock = Lock()

# array for keeping track of other failed processes
failed_p = [False] * 9

# item object stored in priority queue
class item:
	priority = 0     # priority number
	hid = ''         # host_#.message_#
	deliverable = 0  # 1 = message is deliverable, 0 = not deliverable
	message = ''     # string holding message to be displayed in chat room

# Given a packet of data sent by another process, this method parses through
# and returns a tuple containing relevant information. This follows our own
# developed protocol. 
def decode_msg(msg):
	id_string = ""
	priority_string = ""
	index = 0
	op = int(msg[index])
	index += 2
	while msg[index] != '%':
		id_string+=msg[index]
		index += 1
	index += 1
	while msg[index] != '%':
		priority_string+=msg[index]
		index += 1
	priority = float(priority_string)
	index += 1
	message = msg[index:]
	return (op, id_string, priority, message)

# This method adds a message to the queue by appending the correct attributes
# to a new item object. Requires the host #.message # and the message as inputs.
def add_to_queue(hid, msg):
	global seq
	global queue
	seq += 1
	q_item = item()
	q_item.priority = float(seq) + float(host_num)/10
	q_item.hid = hid
	q_item.deliverable = 0
	q_item.message = msg
	queue.append(q_item)

# Removes all items sent by a particular process given the process number.
# Used after detecting failure of a node
def clean_queue(pid):
	sleep(1.5)
	for item in queue:
		if pid == math.floor(float(item.hid)):
			queue.remove(item)
	print "Disconnected from node: " + str(pid)
	
# Handler for properly cleaning up local resources when a failure of another
# node is detected. Creates a thread to clean priority queue, and sets 
# the failed process's index in the global fail_p array to True.
def handle_sendall_error(s, num):
	global response_count
	global failed_p
	if failed_p[num] == True:
		return False
	s.close()
	t = Thread(target = clean_queue, args = (num,))	
	t.start()
	response_count -= 1
	failed_p[num] = True
	return True

# Method for waiting on responses on proposed priorities by other processes
# after multicasting a message to all clients in the chat room.
def wait_responses(hid):
	global max_priority
	global node_count
	global seq
	global response_count
	global client_idx
	message_count = int(hid[2:])
	priority_self = str(seq) + "." + str(host_num)
	priority_self = float(priority_self)
	# maintain correct max priority seen
	if (priority_self > max_priority[message_count % 32]):
		max_priority[message_count % 32] = priority_self
	node_count[message_count % 32] += 1
	# wait until we've received all responses
	while node_count[message_count % 32] != response_count:
		#print "response_count: " + str(response_count)
		i = 1
	# Respond with agreed priority: op = 3
	msg = "3%" + hid + "%" + str(max_priority[message_count % 32]) + "%0" + "#"
	i = 0
	fail_lock.acquire()
	size = len(clients)
	while i < size:
		recipient = clients[i]
		try:
			recipient.sendall(msg)
		except socket.error, e:
			first_error = handle_sendall_error(recipient,client_idx[i])
			if first_error:
				clients.remove(recipient)
				client_idx.remove(client_idx[i])
			i -= 1
			size -= 1
		i +=1
	i = 0
	size = len(client_sockets)
	while i < size:
		recipient = client_sockets[i]
		try:
			recipient.sendall(msg)
		except socket.error, e:
			first_error = handle_sendall_error(recipient,client_sockets_idx[i])
			if first_error:
				client_sockets.remove(recipient)
				client_sockets_idx.remove(client_sockets_idx[i])
			i -= 1
			size -= 1
		i += 1
	fail_lock.release()
	update_queue_item(hid, max_priority[message_count % 32], True)
	node_count[message_count % 32] = 0
	max_priority[message_count % 32] = 0

# Method enabling process to multicast a user-inputted message (string of text) to other
# processes in the chat room. 
def send_msg():
	global client_idx
	# message number, unique to this process
	message_count = 0
	while True:
		# obtain string of text from user
		msg = raw_input("")
		if msg:
			# create full message using protocol
			hid = str(host_num) + "." + str(message_count)
			msg = name + ": " + msg
			message = "1%" + str(host_num) + "." + str(message_count) + "%0%" + msg + "#"
			i = 0
			# obtain lock to enter critical section
			fail_lock.acquire()
			size = len(clients)	
			# multicast to all other members in chat room
			while i < size:
				recipient = clients[i]
				#print "sending to: " + str(client_idx[i])
				try:
					recipient.sendall(message)
				except socket.error, e:
					first_error = handle_sendall_error(recipient, client_idx[i])
					if first_error:
						clients.remove(recipient)
						client_idx.remove(client_idx[i])
					i-=1
					size-=1
				i+=1
			i = 0
			size = len(client_sockets)
			while i < size:	
				recipient = client_sockets[i]
				#print "sending to: " + str(client_sockets_idx[i])
				try:
					recipient.sendall(message)
				except socket.error, e:
					first_error = handle_sendall_error(recipient, client_sockets_idx[i])
					if first_error:
						client_sockets.remove(recipient)
						client_sockets_idx.remove(client_sockets_idx[i])
					i-=1
					size-=1
				i+=1
			# end of critical section
			fail_lock.release()
			add_to_queue(hid, msg)
			t_wait = Thread(target = wait_responses, args = (hid,))
			t_wait.start() 
			message_count += 1

# Method for processing a message received by some other process in the chat room.
# Based on the opcode of the message, we will branch out to the correct set of 
# operations. 
# op = 1: server A has unicasted a message to server B 
# op = 2: server B has responded with a proposed priority on the message sent by server A
# op = 3: server A has sent collected responses and determined an agreed-upon priority
def process_msg(src):
	global max_priority
	global node_count
	global seq
	global client_idx
	global clients
	global client_sockets_idx
	global client_sockets
	# utilize a buffer to collect stream of network data collected from recv()
	buf = ''
	while True:
		try:
			# poll for data
			msg = src.recv(1024)
		except socket.error, e:
			return
		buf+=msg
		while buf:
			# check if this is a full message matching our protocol
			full_msg = buf.find('#')
			if full_msg == -1:
				break
			full_str = buf[0:full_msg]
			(op, hid, priority, message) = decode_msg(full_str)
			# check opcode and branch to correct set of actions
			if op == 1:
				# respond with proposed priority
				add_to_queue(hid, message)
				msg = "2%" + hid + "%" + str(seq) + "."+ str(host_num) + "%0" + "#"
				try:
					src.sendall(msg)
				except socket.error, e:
					fail_lock.acquire()
					first_error = handle_sendall_error(src, int(hid))
					socket_num = int(hid)
					if first_error:
						if host_num < socket_num:
							for i, value in enumerate(client_idx):
								if value == socket_num:
									client_idx.remove(value)
									clients.remove(clients[i])
									fail_lock.release()
									return
						else:
							for i, value in enumerate(client_sockets_idx):
								if value == socket_num:
									client_sockets_idx.remove(value)
									client_sockets.remove(client_sockets[i])
									fail_lock.release()
									return
					fail_lock.release()	
			elif op == 2:
				# collect response and increment responses seen in appropriate data structures
				message_count = int(hid[2:])
				if (priority > max_priority[message_count % 32]):
					max_priority[message_count % 32] = priority
				node_count[message_count % 32] += 1
			elif op == 3:
				# acquire agreed priority and mark item as deliverable
				update_queue_item(hid, priority, False)
			buf = buf[full_msg+1:]

# Helper function for updating attributes of an item in the priority queue.
# Function will iteratively loop through priority queue until desired item is seen.
# Will keep priority queue sorted based on the priority attribute. 
def update_queue_item(hid, new_priority, self):
	global queue
	global seq
	# loop through all items in queue
	for i in queue:
		# examine message id
		if i.hid == hid:
			i.priority = new_priority
			# maintain deliverability attribute
			if i.deliverable == 0:
				i.deliverable = 1
				msg = "3%" + hid + "%" + str(new_priority) + "%0" + "#"
				j = 0
				# acquire lock to enter critical section
				fail_lock.acquire()
				size = len(clients)
				while j < size:
					recipient = clients[j]
					try:
						recipient.sendall(msg)
					except socket.error, e:
						first_error = handle_sendall_error(recipient, client_idx[j])
						if first_error:
							clients.remove(recipient)
							client_idx.remove(client_idx[j])
						j -= 1
						size -= 1
					j += 1
				j = 0
				size = len(client_sockets)
				while j < size:
					recipient = client_sockets[j]
					try:
						recipient.sendall(msg)
					except socket.error, e:
						first_error = handle_sendall_error(recipient, client_sockets_idx[j])
						if first_error:
							client_sockets.remove(recipient)
							client_sockets_idx.remove(client_sockets_idx[j])
						j -= 1
						size -= 1
					j += 1
				# end of critical section
				fail_lock.release()
			else:
				i.deliverable = 1
			break
	# sort queue based on priority attribute of item object
	queue.sort(key = lambda x: x.priority)
	# correctly update local sequence number
	if new_priority > seq:
		dp_idx = str(new_priority).find('.')
		dp = int( str(new_priority)[dp_idx+1:] )
		seq = int(new_priority)
		if dp > host_num:
			seq+=1
	# print all messages that are deliverable and at the head of the queue
	while queue:
		if queue[0].deliverable == 1: 
			item = queue.pop(0)
			print item.message
		else:
			break

# gets client number given a host name
def get_client_num(client_name):
	return int(client_name[16])

# server functionality for accepting connections from clients
def server(s):
	global response_count
	global client_idx
	print "Accepting connections from clients ... "
	while True:
		c, addr = s.accept()
		print "Accepted connection from: ", addr
		clients.append(c)
		client_name = socket.gethostbyaddr(addr[0])[0]
		client_num = int(client_name[16])
		client_idx.append(client_num)
		response_count += 1


# obtain user's name to be used in chat room		
name = raw_input("Enter your chat name: ")

# socket initialization
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = socket.gethostname()
host_num = int(host[16])
clients = []
port = 10001

# chat room initilization
s.bind((host, port))
s.listen(5)
sleep(8)

# create thread to act as server
t_server = Thread(target = server, args = (s,))
t_server.start()

# may need a different socket per connection to servers
client_sockets = []

print "Initiating connections to servers ... "
for i in range (1, host_num):
	client_s = socket.socket()
	client_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	host = "sp17-cs425-g11-0"+str(i)+".cs.illinois.edu"
	client_s.connect((host, port))
	client_sockets.append(client_s)
	client_sockets_idx.append(i)
	response_count += 1
sleep(10)
print "Finished attempts to make connections ... "
#print clients
#print client_sockets
print "\n-------------- CHAT ROOM --------------\n" 



# multithread delivering msgs from clients list
for c in clients:
	t = Thread(target = process_msg, args = (c,))
	t.start()

# multithread delivering msgs from client_sockets
for c in client_sockets:
	t = Thread(target = process_msg, args = (c,))
	t.start()

# thread for sending msgs to other members in chat room 
send_msg()

while True:
	i = 1	


