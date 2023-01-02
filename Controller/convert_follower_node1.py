import json
import socket
import traceback
import time
import threading

# Wait following seconds below sending the controller request
time.sleep(5)

# Read Message Template
msg = json.load(open("Message.json"))
leader = 'Node1'

def listener(skt):
    global leader 
    print(f"Starting Listener from ", sender)
    while True:
        try:
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from all nodes
        decoded_msg = json.loads(msg.decode('utf-8'))
        msg_sender = decoded_msg['sender_name']
        request = decoded_msg['request']
        leader = decoded_msg['value']
        # print(f"Message Received : {decoded_msg} From : {addr}")
        print(request, " received from node ",msg_sender," Leader Node is ",leader)


# Convert Nodes to Followers
sender = "Controller"
port = 5555

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))

# Listeneer Thread
threading.Thread(target=listener, args=[skt]).start()

targets=["Node1","Node2","Node3","Node4","Node5"]
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['request'] = "CONVERT_FOLLOWER"
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")



time.sleep(5)
# Timing out a node to start the election

sender = "Controller"
target = "Node1"
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "TIMEOUT"
print(f"Request Created : {msg}")

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

time.sleep(10)
# for i in range(5):
#     sender = "Controller"
#     target = targets[i]
#     port = 5555

#     msg['sender_name'] = sender
#     msg['request'] = "LEADER_INFO"
#     print(f"Request Created : {msg}")

#     try:
#         # Encoding and sending the message
#         skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
#     except:
#         #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
#         print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


#Store command 
time.sleep(30)
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['term'] = None
    msg['request'] = "STORE"
    msg['key'] = "Store_Key1"
    msg['value'] = "Controller_Value 1"
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

time.sleep(30)
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['term'] = None
    msg['request'] = "STORE"
    msg['key'] = "Store_Key2"
    msg['value'] = "Controller_Value 2"
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


time.sleep(50)

#Shutting Down Leader Node
sender = "Controller"
port = 5555

# Request
msg['sender_name'] = sender
msg['request'] = "SHUTDOWN"
print(f"Request Created : {msg}")
dead_node = leader
# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (leader, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

#Adding more entries to the log

time.sleep(80)
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['term'] = None
    msg['request'] = "STORE"
    msg['key'] = "Store_Key3"
    msg['value'] = "Controller_Value 3"
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

time.sleep(50)
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['term'] = None
    msg['request'] = "STORE"
    msg['key'] = "Store_Key4"
    msg['value'] = "Controller_Value 4"
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

time.sleep(50)

#Retrieveing logs from nodes
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['term'] = None
    msg['request'] = "RETRIEVE"
    msg['key'] = None
    msg['value'] = None
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

time.sleep(50)
#Restarting the old node
sender = "Controller"
target = dead_node
port = 5555

msg['sender_name'] = sender
msg['request'] = "START_NODE"
print(f"Request Created : {msg}")
try:
        # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

time.sleep(50)
time.sleep(10)
#Adding new logs
for i in range(5):
    sender = "Controller"
    target = targets[i]
    port = 5555

    msg['sender_name'] = sender
    msg['term'] = None
    msg['request'] = "STORE"
    msg['key'] = "Store_Key5"
    msg['value'] = "Controller_Value 5"
    print(f"Request Created : {msg}")

    try:
        # Encoding and sending the message
        skt.sendto(json.dumps(msg).encode('utf-8'), (target, port))
    except:
        #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

