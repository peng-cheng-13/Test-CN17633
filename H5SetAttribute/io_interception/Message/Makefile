.PHONY:all
all:server client
 
client:client.c comm.c
	g++ -std=c++11 -o $@ $^
server:server.c comm.c
	g++ -std=c++11 -o $@ $^
 
.PHONY:clean
clean:
	rm -f client server
