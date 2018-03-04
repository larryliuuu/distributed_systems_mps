import socket
import sys
#import copy
from threading import Thread, Lock, Event # Import thread library
from time import sleep, time

trans_flag = False # flag for restarting transaction when a transaction has been aborted
ack_flag = Event()

# formats message to fit our messaging protocol
def format_msg(op, server_num, key, value, client_id, trans_id):
	msg = str(op) + "`" + str(server_num) + "`" + str(key) + "`" +str(value) + "`" + str(client_id) + "`" + str(trans_id) + "~"
	return msg 

# main method for processing operations when a transaction has been started
def begin_transaction():
	global trans_flag
	global ack_flag
 	
	# send BEGIN to coordinator
	msg = format_msg(0,0,"g","g",host_num,0)
	send_msg(msg)

	# while transaction should exist process user-typed commands and send to coordinator
	while True:
		
		# acquire user typed input
		user_cmd = raw_input("")

		# parse command
		space1 = user_cmd.find(' ')
		space2 = user_cmd.find(' ', space1+1)
		dot = user_cmd.find('.')
		cmd = user_cmd[0:space1]

		if(cmd == "SET"):
			server_name = user_cmd[space1+1:dot]
			# find server number to send operation to
			server_num = ord(server_name) - ord('A')
			key = user_cmd[dot+1:space2]
			value= user_cmd[space2+1:]
			msg = format_msg(1,server_num,key,value,host_num,0)
			send_msg(msg)
		elif(cmd == "GET"):
			server_name = user_cmd[space1+1:dot]
			server_num = ord(server_name) - ord('A')
			key = user_cmd[dot+1:]
			msg = format_msg(2,server_num,key,"g",host_num,0)
			send_msg(msg)
		elif(user_cmd == "COMMIT"):
			msg = format_msg(3,0,"g","g",host_num,0)
			send_msg(msg)
			trans_flag = False
		elif(user_cmd== "ABORT"):
			msg = format_msg(4,0,"g","g",host_num,0)
			send_msg(msg)
			trans_flag = False
		else:
			print "Invalid command"
			sys.stdout.flush()

		ack_flag.wait()
		ack_flag.clear()
		if trans_flag is False:
			break
		
# read message and modify any relevant flags	
def read_msg(src):
	global trans_flag
	global ack_flag
	# utilize a buffer to collect stream of network data collected from recv()
	buf = ''
	while True:
		try:
			# poll for data
			msg = src.recv(512)
		except socket.error, e:
			print "ERROR: Coordinator Connection Lost :("
			return
		buf+=msg
		while buf:
			# check if this is a full message matching our protocol
			full_msg = buf.find('~')
			if full_msg == -1:
				break
			full_str = buf[0:full_msg]
			# check whether we should abort any current transactions
			if full_str == "NOT FOUND" or full_str == "ABORT":
				trans_flag = False
			# print result of an operation
			print full_str
			sys.stdout.flush()
			ack_flag.set()
			buf = buf[full_msg+1:]

# simply sends msg to coordinator
def send_msg(msg):
	#print msg
	try:
		s.sendall(msg)
	except socket.error, e:
		i = 1
		#print "send_msg error"

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

# socket initialization
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = socket.gethostname()
host_num = int(host[16])
port = 10001
ack_flag.clear()

#print "Initiating connection to coordinator ... "
#sys.stdout.flush()
coord_ip = "sp17-cs425-g11-09.cs.illinois.edu"
while True:
	try:
		s.connect((coord_ip, port))
		break
	except socket.error, e:
		a = 1

#print "\n-------------- TRANSACTIONS --------------\n" 
#sys.stdout.flush()
#print "--------My node is: " + str(host_num) + "--------\n"
#sys.stdout.flush()


t_read = Thread(target = read_msg, args = (s,))
t_read.start()

while True:
	# wait for user to type BEGIN to begin transaction
	user_cmd = raw_input("")
	if user_cmd == "BEGIN":
		print "OK"
		sys.stdout.flush()
		trans_flag = True
		ack_flag.clear()
		begin_transaction()
	else:
		print "INVALID INSTRUCTION: Enter BEGIN to open a new transaction"
		sys.stdout.flush()	








