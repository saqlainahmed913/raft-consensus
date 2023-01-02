import random
import socket
import time
import threading
import json
import traceback
import os


def timeout():
    x = random.randint(30,100)
    return x

def election(sender):
    global term
    vote_req = json.load(open("Message.json"))
    vote_req['sender_name'] = sender
    vote_req['request'] = "VOTE_REQUEST"
    vote_req['term'] = term
    vote_req['key'] = None
    vote_req['value'] = None
    vote_req['candidateId'] = sender
    vote_req['prevLogIndex'] = prevLogIndex
    vote_req['prevLogTerm'] = prevLogTerm

    votereq_bytes = json.dumps(vote_req).encode()
    return votereq_bytes

def vote_ack(sender):
    global term,electionTimeout
    term = candidate_term
    electionTimeout = timeout()
    ack_msg = json.load(open("Message.json"))
    ack_msg['sender_name'] = sender
    ack_msg['request'] = "VOTE_ACK"
    ack_msg['term'] = term
    ack_msg['key'] = None
    ack_msg['value'] = None

    ackmsg_bytes = json.dumps(ack_msg).encode()
    return ackmsg_bytes

def heartbeat(sender,logEntry):
    msg = json.load(open("Message.json"))
    msg['sender_name'] = sender
    msg['request'] = "APPEND_RPC"
    msg['term'] = term
    msg['key'] = None
    msg['value'] = None
    msg['leaderId'] = sender
    msg['prevLogIndex'] = prevLogIndex
    msg['prevLogTerm'] = prevLogTerm
    msg['entry'] = logEntry
    msg['commitIndex'] = commitIndex

    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

def heartbeat_reply(sender,success,entry):
    msg = json.load(open("Message.json"))
    msg['sender_name'] = sender
    msg['request'] = "APPEND_REPLY"
    msg['term'] = term
    msg['leaderId'] = currLeader
    msg['key'] = None
    msg['value'] = None
    msg['success'] = success
    msg['entry'] = entry
    msg['commitIndex'] = commitIndex

    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

def leader_info(sender, currLeader):
    msg = json.load(open("Message.json"))
    msg['sender_name'] = sender
    msg['request'] = "LEADER_INFO"
    msg['term'] = None
    msg['key'] = 'LEADER'
    msg['value'] = currLeader


    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

def retrieve_reply(sender):
    msg = json.load(open("Message.json"))
    msg['sender_name'] = sender
    msg['request'] = "RETRIEVE"
    msg['term'] = None
    msg['key'] = "COMMITTED_LOGS"
    msg['value'] = logArray

    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

def send_msg(sender,target):
    global term,msg_sender,vote_flag,term,isCandidate,isLeader,isFollower,logArray,nextIndex,matchIndex,commitIndex,prevLogIndex,prevLogTerm,commitCount
    print(f"Starting Sender from",sender, " \n")

    #sending heartbeat as leader
    while True:
        if isLeader == True:   #create a is leader var 
            commitCount = 0
            for i in range(len(target)):
                if sender != target[i]:
                    # print("Sending Heartbeat from",sender," to ",target[i])
                    # print(logArray)
                    prevLogIndex = nextIndex[i]
                    if(nextIndex[i]>len(logArray)):
                        node_msg = heartbeat(sender,[])
                    else:
                        print('Latest entry',logArray[nextIndex[i]-1])
                        # print('Next Index of follower',nextIndex[i])
                        node_msg = heartbeat(sender,logArray[nextIndex[i]-1]) 
                        logValue = logArray[nextIndex[i]-1]
                        prevLogTerm = logValue['Term']
                    UDP_Socket.sendto(node_msg, (target[i], 5555))
            time.sleep(rpc_int)

        #sending vote request to followers
        elif isCandidate == True:
            term+=1
            isCandidate= False
            for i in range(len(target)):
                if sender != target[i]:
                    # print("Sending Vote Request from",sender," to ",target[i])
                    node_msg = election(sender)
                    UDP_Socket.sendto(node_msg, (target[i], 5555))

        #sending vote acknowledgement to the candidate
        elif (isFollower == True or isLeader==True) and vote_flag == 1:
            if(isLeader==True):
                isLeader=False
                isFollower=True
                isCandidate=False
            vote_flag = 0
            vote = vote_ack(sender)
            # print("Sending Vote from",sender," to ",msg_sender)
            print("New timeout",electionTimeout)
            UDP_Socket.sendto(vote, (msg_sender, 5555))


    
    print(f"\n All Messages sent from ",sender)

def checkElectionTimeout(sender):
    global term,electionTimeout,isCandidate,isFollower,isLeader
    while True:
        # convert follower to candidate
        if( electionTimeout < time.time() - node_time and isFollower==True):
            isCandidate=True
            isFollower=False
            isLeader=False

def conductElection(sender):
    global term,node_time,isCandidate,votes,isFollower,isLeader,active_nodes,election_flag,logArray,nextIndex
    votes+=1
    while True :
        if(election_flag==1):
            if(votes>(active_nodes/2.0)):
                # print("Leader elected", sender)
                if(len(logArray)!=0):
                    nextIndex = [len(logArray)+1,len(logArray)+1,len(logArray)+1,len(logArray)+1,len(logArray)+1]
                election_flag=0
                isLeader = True
                isCandidate = False
                isFollower = False


def listener(skt, currnode):
    global term,node_time,vote_flag,votes,voted_for,election_flag,isFollower,isCandidate,isLeader,msg_sender,active_nodes,currLeader,logArray,commitIndex,nextIndex,matchIndex,commitCount,candidate_term
    currLeader = None
    print(f"Starting Listener from ", currnode)
    # count = 0
    while True:
        try:
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from all nodes
        decoded_msg = json.loads(msg.decode('utf-8'))
        msg_request = decoded_msg['request']
        msg_sender = decoded_msg['sender_name']
        msg_sender_term = decoded_msg['term']

        if(msg_request == 'STORE'):
            if(isLeader==True):
                dict = {'Term':term,'Key':decoded_msg['key'],'Value':decoded_msg['value']}
                logArray.append(dict)
            elif(isFollower==True or isCandidate==True):
                target = msg_sender
                node_msg = leader_info(sender, currLeader)
                UDP_Socket.sendto(node_msg, (target, 5555))

        elif(msg_request == 'RETRIEVE'):
            if(isLeader==True):
                target = msg_sender
                node_msg = retrieve_reply(sender)
                UDP_Socket.sendto(node_msg, (target, 5555))
            elif(isFollower==True or isCandidate==True):
                target = msg_sender
                node_msg = leader_info(sender, currLeader)
                UDP_Socket.sendto(node_msg, (target, 5555))

        elif(msg_request == 'APPEND_RPC'):
            if(isFollower==True):
                currLeader = decoded_msg['leaderId']
                print("Received Heartbeat from ",msg_sender)
                msg_index = decoded_msg['prevLogIndex']
                msg_term = decoded_msg['prevLogTerm']
                success = False
                print('Follower Logs',logArray)
                if((msg_sender_term >= term)):
                    print('Received Entry',decoded_msg['entry'])
                    if(msg_index >= len(logArray)):
                        if(decoded_msg['entry']!=[]):
                            logArray.append(decoded_msg['entry'])
                        commitIndex = min(decoded_msg['commitIndex'],len(logArray))
                        success = True
                target = msg_sender
                node_msg = heartbeat_reply(sender,success,decoded_msg['entry'])
                UDP_Socket.sendto(node_msg, (target, 5555))
                
            if(isCandidate==True):
                currLeader = msg_sender['leaderId']
                isCandidate = False
                isFollower = True
                isLeader = False
                election_flag = 0
                votes=0
            node_time=time.time()
        # change nextIndex, matchIndex and check commitIndex condition
        elif(msg_request == 'APPEND_REPLY'):
            if(isLeader == True):
                currSender = str(msg_sender)
                i = (int(currSender[-1])) - 1
                if(decoded_msg['success'] == True):
                    if(decoded_msg['entry']!=[]):
                        nextIndex[i] = nextIndex[i] + 1
                        # matchIndex[i] = matchIndex[i] + 1
                        matchIndex[i] = decoded_msg['commitIndex']
                        commitCount += 1
                        # commitIndex updation
                        if(commitCount == (totalNodes-1)/2):
                            commitIndex = decoded_msg['commitIndex']
                else:
                    nextIndex[i] = nextIndex[i] - 1

        elif(msg_request == 'VOTE_REQUEST' ):
            candidate_term = msg_sender_term
            if(isFollower==True or isLeader==True):
                print("Received Vote Request from ",msg_sender)
                node_time=time.time()
                voterLogIndex = len(logArray)-1
                # voterLogTerm = logArray[len(logArray)-1]['Term']
                candidateLogIndex = decoded_msg['prevLogIndex'] - 1
                # candidateLogTerm = decoded_msg['prevLogTerm']
                if((msg_sender_term>term) or (msg_sender_term == term and voterLogIndex < candidateLogIndex)):
                    voted_for = msg_sender
                    vote_flag = 1
                else:
                    print('Denied vote to', msg_sender)
                    voted_for = None
                    vote_flag = 0

        elif(msg_request == 'VOTE_ACK'):
            print("Received Vote from ",msg_sender)
            votes+=1
            election_flag=1

        #Convert to follower from controller
        elif(msg_request == 'CONVERT_FOLLOWER'):
            print("Convert follower request")
            isFollower = True
            isCandidate = False
            isLeader = False

        #Timeout the node from controller 
        elif(msg_request == 'TIMEOUT'):
            print("Timeout Request")
            isCandidate = True
            isFollower = False
            isLeader = False

        #Shutdown all threads from controller            
        elif(msg_request == 'SHUTDOWN'):
            print("Shutdown Request")
            isCandidate=False
            isFollower=False
            isLeader=False
            vote_flag=0
            election_flag=0
            active_nodes-=1

        #Starting the dead node
        elif(msg_request == 'START_NODE'):
            if (isCandidate == False and isFollower == False and isLeader == False):
                print("Start request ")
                isFollower = True
                term = candidate_term
                active_nodes+=1
                node_time=time.time()


        #send leader info to controller
        elif(msg_request == 'LEADER_INFO'):
            if isLeader == True:
                target = msg_sender
                node_msg = leader_info(sender)
                UDP_Socket.sendto(node_msg, (target, 5555))

            
        # print(f"Message Received : {decoded_msg} From : {addr}")

if __name__ == "__main__":

    global term,voted_for,rpc_int,electionTimeout,sender,active_nodes,votes,vote_flag,election_flag,node_time,isLeader,isCandidate,isFollower,logArray,nextIndex,matchIndex,commitIndex,prevLogIndex,prevLogTerm,commitCount,totalNodes  #current term, start as 0 
      #heartbeat interval
    term = 0
    voted_for = None
    rpc_int = 10
    votes=0
    vote_flag = 0
    election_flag=0
    electionTimeout = timeout()
    print(electionTimeout)
    node_time=time.time()
    isLeader = False
    isFollower = False
    isCandidate = False
    logArray = []
    nextIndex = [1,1,1,1,1]
    matchIndex = [0,0,0,0,0]
    commitIndex = 0
    prevLogIndex = 0
    prevLogTerm = 0
    commitCount = 0
    

    print("Starting Node")

    sender = os.environ['container_name']
    target = ["Node1","Node2","Node3","Node4","Node5"]
    active_nodes = len(target)
    totalNodes = len(target)

    # Creating Socket and binding it to the target container IP and port
    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    # Bind the node to sender ip and port
    UDP_Socket.bind((sender, 5555))

    #Starting thread 1
    threading.Thread(target=listener, args=[UDP_Socket,sender]).start()  
    time.sleep(10)
    
    #ElectionTimeoutCheck thread
    threading.Thread(target=checkElectionTimeout,args=[sender]).start()

    #LeaderElection thread
    threading.Thread(target=conductElection,args=[sender]).start()

    #Starting thread 4
    threading.Thread(target=send_msg,args=[sender,target]).start()
    
    time.sleep(10)

