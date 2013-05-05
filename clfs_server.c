#include <stdio.h>   
#include <sys/types.h>   
#include <sys/socket.h>   
#include <netinet/in.h>   
#include <arpa/inet.h> 
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <math.h>

#define PORT 8888
#define MAX_PENDING_CONNECTIONS SOMAXCONN
#define REQ_SIZE_32BIT 12

enum clfs_type {
	CLFS_PUT,
	CLFS_GET,
	CLFS_RM
};

struct clfs_req {
	enum clfs_type type;
	unsigned int inode;
	unsigned int size;
  };

enum clfs_status {
	CLFS_OK = 0,            /* Success */
	CLFS_INVAL = 22,    /* Invalid address */
	CLFS_ACCESS = 13,   /* Could not read/write file */
	CLFS_ERROR              /* Other errors */
};

struct evict_page {
	char data[4096];
	int end;
};

pthread_mutex_t work_mutex;
struct sockaddr_in servip, clntip;
void *pthread_fn(void *arg);
void send_status(int new_fd, enum clfs_status status);

int main(int argc, char** argv)
{
	int socketfd, new_fd;
	int rtn;	
	pthread_t a_thread;
	socklen_t addrlen = (socklen_t) sizeof(struct sockaddr);

	/* Initiate the listening socket */
	socketfd = socket(AF_INET,SOCK_STREAM,0);
	if(socketfd==-1)
	{
		perror("Error occured in socket\n");
		exit(1);
	}	
	printf("socketfd=%d\n",socketfd);

	int tr=1;

        /* kill "Address already in use" error message */
	if (setsockopt(socketfd,SOL_SOCKET,SO_REUSEADDR,&tr,sizeof(int)) == -1) {
		perror("setsockopt");
		exit(1);
	}


	bzero(&servip,sizeof(struct sockaddr_in));

	servip.sin_family=AF_INET;
	servip.sin_addr.s_addr= INADDR_ANY;
	servip.sin_port=htons(PORT);
	
	/* Bind to port PORT */
	rtn = bind(socketfd, (const struct sockaddr*)&servip, sizeof(struct sockaddr));
	if (rtn) {
		perror("Error occured in bind\n");
		exit(1);	
	}

	/* Set up listening */
	rtn=listen(socketfd, MAX_PENDING_CONNECTIONS);
	if (rtn) {
		perror("Error occured in listen\n");
		exit(1);	
	}

	while (1) {	
		printf("Waiting for connection...\n");
		new_fd = accept(socketfd,(struct sockaddr *)&clntip,&addrlen);
		if (new_fd == -1) {
				perror("Error occured in accept\n");
				continue;
		}
		printf("Client connected on socket %d...\n", new_fd);	
		rtn = pthread_create(&a_thread, NULL, pthread_fn, &new_fd);

	 	if (rtn) {
			perror("Error occured in pthread_create\n");
			continue;
		}
	}
	close(socketfd);
}


static int __recv_file(int sockfd, char * path, unsigned int size) {
	int r;
	struct evict_page *page_buf = malloc(sizeof(struct evict_page));
	size_t buflen;
	struct stat st;
	int count = 0;

	FILE *fp = fopen((const char *)path, "w+");
	if (fp == NULL) {
		r = CLFS_ACCESS;
		goto out_page_buf;
	}
	
	while (1) {
		r = recv(sockfd, page_buf, sizeof(struct evict_page), MSG_WAITALL);
		if (r < sizeof(struct evict_page)) {
			printf("Receive %d pages in total\n", count);
			r =  CLFS_ERROR;
			goto out_fp;
		}
		count++;
		if (page_buf->end) {
			buflen = page_buf->end;
		} else {
			buflen = 4096;
			send_status(sockfd, CLFS_OK);			
		}

		r = fwrite((const char*) (page_buf->data), 1, buflen, fp);
		if (r < buflen) {
			printf("Receive %d pages in total\n", count);
			r =  CLFS_ACCESS;
			goto out_fp;
		}
		
		if (page_buf->end)
			break;
	} 
	
	printf("Receive %d pages in total\n", count);

	stat(path, &st);
	if (size > st.st_size)
		r =  CLFS_ERROR;
	else
		r =  CLFS_OK;
out_fp:
	fclose(fp);
out_page_buf:
	free(page_buf);
	return r;
}

void *pthread_fn(void *arg)
{
	struct clfs_req req;
	int rtn;
	FILE *fp;
	int new_fd = *(int *)arg;
	char *path = malloc(30);

	/* Receive clfs_req from client */
	rtn = recv(new_fd, &req, sizeof(struct clfs_req), 0);

	if(rtn != REQ_SIZE_32BIT)
	{
		send_status(new_fd, CLFS_ERROR);
		goto end_all;
	}

	
	sprintf(path, "clfs_store/%d.dat", req.inode);
	send_status(new_fd, CLFS_OK);

	if (req.type == CLFS_PUT) {
		printf("Receive PUT request\n");
		if (req.size > 0) {
			rtn = __recv_file(new_fd, path, req.size);
			send_status(new_fd, rtn);
			exit(1);
		}
	}

	if (req.type == CLFS_GET) {
		printf("Receive GET request\n");
		unsigned char *data = malloc(req.size);

		fp = fopen(path, "r");
		if (fp) {
			send_status(new_fd, CLFS_OK);
		}
		else {
			send_status(new_fd, CLFS_INVAL);
			goto end_GET;
		}

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
	}

	if (req.type == CLFS_RM) {
		rtn = remove(path);
 		if(rtn != 0 ) {
 			send_status(new_fd, CLFS_INVAL);
 			goto end_RM;
 		}
		printf("%s file deleted successfully.\n", path);
		send_status(new_fd, CLFS_OK);
	}
end_RM:
end_all:
	free(path);
	close(new_fd);
	printf("Thread about to exit\n");
	pthread_exit(NULL);	
}

void send_status(int new_fd, enum clfs_status status) {
	send(new_fd, &status, sizeof(status), 0);
	switch(status) {
	case CLFS_OK:
		break;
	case CLFS_INVAL:
		perror("Invalid address\n");
		break;
	case CLFS_ACCESS:
		perror("Couldn't read/write (length does not match)\n");
		break;
	case CLFS_ERROR:
		perror("Error occured in communicating\n");
		break;
	default:
		perror("Fine!\n");
	}
}
