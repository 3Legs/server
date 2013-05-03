#include <stdio.h>   
#include <sys/types.h>   
#include <sys/socket.h>   
#include <netinet/in.h>   
#include <arpa/inet.h> 
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <math.h>

#define PORT 8888
#define MAX_PENDING_CONNECTIONS SOMAXCONN

struct clfs_req {
	enum {
	      CLFS_PUT,
	      CLFS_GET,
	      CLFS_RM
	} type;
	int inode;
	int size;
  };

enum clfs_status {
	CLFS_OK = 0,            /* Success */
	CLFS_INVAL = 22,    /* Invalid address */
	CLFS_ACCESS = 13,   /* Could not read/write file */
	CLFS_ERROR              /* Other errors */
};

pthread_mutex_t work_mutex;
struct sockaddr_in servip, clntip;
void *pthread_fn(void *arg);
void send_status(int new_fd, enum clfs_status status);

void main(void)
{
	int socketfd, new_fd, addrlen;
	int rtn;	
	pthread_t a_thread;


	/* Initiate the listening socket */
	socketfd = socket(AF_INET,SOCK_STREAM,0);
	if(socketfd==-1)
	{
		perror("Error occured in socket\n");
		exit(1);
	}	
	printf("socketfd=%d\n",socketfd);

	int tr=1;

// kill "Address already in use" error message
	if (setsockopt(socketfd,SOL_SOCKET,SO_REUSEADDR,&tr,sizeof(int)) == -1) {
		perror("setsockopt");
		exit(1);
	}


	bzero(&servip,sizeof(struct sockaddr_in));

	servip.sin_family=AF_INET;
	servip.sin_addr.s_addr= INADDR_ANY;
	servip.sin_port=htons(PORT);
	// bzero(&(servip.sin_zero), 8);
	
	/* Bind to port PORT */
	rtn = bind(socketfd, (const struct sockaddr*)&servip, sizeof(struct sockaddr_in));
	if(rtn==-1)
	{
		perror("Error occured in bind\n");
		exit(1);	
	}

	/* Set up listening */
	rtn=listen(socketfd, MAX_PENDING_CONNECTIONS);
	if(rtn==-1)
	{
		perror("Error occured in listen\n");
		exit(1);	
	}

	while(1)
	{	
		printf("Waiting for connecting...\n");
		addrlen = sizeof(clntip);
		new_fd = accept(socketfd,(struct sockaddr *)&clntip,&addrlen);
		if(new_fd == -1) {
				perror("Error occured in accept\n");
				close(socketfd);
				exit(1);
			}
		printf("Client connected...\n");		
		/* Create new thread */		
		rtn=pthread_create(&a_thread,NULL, pthread_fn, &new_fd);

	 	if(rtn!=0)
			{
				perror("Error occured in pthread_create\n");
				close(new_fd);
				close(socketfd);
				break;
			}
	}
	close(socketfd);
}

void *pthread_fn(void *arg)
{
	struct clfs_req req;
	int rtn;
	int new_fd = *(int *)arg;
	enum clfs_status status = CLFS_OK;
	FILE *fp;
	char *path;

	/* Receive clfs_req from client */
	rtn = recv(new_fd, &req, sizeof(struct clfs_req), 0);
	if(rtn < sizeof(struct clfs_req) || (req.type != CLFS_PUT && req.type != CLFS_GET && req.type != CLFS_RM))
	{
		send_status(new_fd, CLFS_ERROR);
		pthread_exit(NULL);
	}

	/* Send OK to client */
	send_status(new_fd, CLFS_OK);

	if (req.type == CLFS_PUT) {
		printf("Receive PUT request\n");
		unsigned char *data = malloc(req.size);

		/* Receive data from client */
		rtn = recv(new_fd, data, req.size, 0);
		if (rtn != req.size) {
			send_status(new_fd, CLFS_ERROR);
			goto end_PUT;
		}
		
		path = (char *) malloc(12+log10(req.inode));
		sprintf(path, "clfs_store/%d.dat", req.inode);

		/* open the file */
		fp = fopen((const char *)path, "w+");
		if (fp == NULL) {
			send_status(new_fd, CLFS_INVAL);
			goto end_PUT;
		}

		/* write to the file */
		rtn = fwrite((const char *)data, 1, req.size, fp);
		if (rtn != req.size) {
			send_status(new_fd, CLFS_ACCESS);
			goto end_PUT;
		}

		/* Send OK to client */
		send_status(new_fd, CLFS_OK);

		/* Exit the thread */
		end_PUT:
		fclose(fp);
		free(data);
		free(path);
		pthread_exit(NULL);	
	}

	if (req.type == CLFS_GET) {
		printf("Receive GET request\n");
		unsigned char *data = malloc(req.size);
		path = (char *) malloc(12+log10(req.inode));
		sprintf(path, "clfs_store/%d.dat", req.inode);

		if (fp = fopen(path, "r")) {
	        send_status(new_fd, CLFS_OK);
	    }
	    else {
	    	send_status(new_fd, CLFS_INVAL);
	    	goto end_GET;
	    }
/*
	    fseek(fp, 0, SEEK_END);
    	int len = (int) ftell(f);
    	if (len != req.size) {
    		perror("Size does not match\n");
    		status = CLFS_ACCESS;
			rtn = send(new_fd, status, sizeof(status), 0);
			exit(1);
    	}
*/
    	rtn = fread(data, 1, req.size, fp);
    	if (rtn != req.size) {
    		send_status(new_fd, CLFS_ACCESS);
    		goto end_GET;
    	}

    	/* Send OK and data to client */
		send_status(new_fd, CLFS_OK);

		/* Exit the thread */
		end_GET:
		fclose(fp);
		free(data);
		free(path);
		pthread_exit(NULL);
	}

	if (req.type == CLFS_RM) {
		path = (char *) malloc(12+log10(req.inode));
		sprintf(path, "clfs_store/%d.dat", req.inode);
		rtn = remove(path);
 		if(rtn != 0 ) {
 			send_status(new_fd, CLFS_INVAL);
 			goto end_RM;
 		}
		printf("%s file deleted successfully.\n", path);

		/* Send OK and data to client */
		send_status(new_fd, CLFS_OK);

		/* Exit the thread */
		end_RM:
		free(path);
		pthread_exit(NULL);
		}

}

void send_status(int new_fd, enum clfs_status status) {
	switch(status) {
		case CLFS_OK:
			send(new_fd, &status, sizeof(status), 0);
			break;
		case CLFS_INVAL:
			perror("Invalid address\n");
			send(new_fd, &status, sizeof(status), 0);
			break;
		case CLFS_ACCESS:
			perror("Couldn't read/write (length does not match)\n");
			send(new_fd, &status, sizeof(status), 0);
			break;
		case CLFS_ERROR:
			perror("Error occured in communicating\n");
			send(new_fd, &status, sizeof(status), 0);
			break;
		default:
			perror("Error occured in status\n");
	}
}