all:
	gcc clfs_server.c -lm -lpthread -o server
clean:
	@rm -rf *.o
	@rm server
