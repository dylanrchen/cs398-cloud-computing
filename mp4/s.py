import socket
import time

# create an INET, STREAMing socket
# bind the socket to a public host, and a well-known port
# become a server socket
# accept connections from outside
# now do something with the clientsocket
# in this case, we'll pretend this is a threaded server

dicttemplate = {
    "twitter": """{"text": "<t#weet body>"}""",
    "reddit": """{"text": "<com#ment b#ody>","subreddit":"""
    """"<subreddit name>","author": "<comment author>"}"""
}

output = "reddit"

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serversocket:
    serveraddr = ("localhost", 49225)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind(serveraddr)
    serversocket.listen(5)
    while True:
        print(serveraddr)
        clientsocket, address = serversocket.accept()
        with clientsocket:
            sent = dicttemplate[output]+"\n"
            intv = 0.3
            try:
                # for i in range(int(10/intv)):
                while True:
                    clientsocket.send(sent.encode())
                    time.sleep(intv)
            except BrokenPipeError:
                pass
