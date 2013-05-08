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
#define SEND_SIZE 4096

enum clfs_type {
	CLFS_PUT,
	CLFS_GET,
	CLFS_RM
};

struct clfs_req {
	enum clfs_type type;
	uint32_t inode;
	uint32_t size;
};

enum clfs_status {
	CLFS_OK = 0,		/* Success */
	CLFS_INVAL = 22,	/* Invalid address */
	CLFS_ACCESS = 13,	/* Could not read/write file */
	CLFS_ERROR		/* Other errors */
};

pthread_mutex_t work_mutex;
struct sockaddr_in servip, clntip;
void *pthread_fn(void *arg);
void send_status(int new_fd, enum clfs_status status);
enum clfs_status read_status(int fd);

int
main(int argc, char **argv)
{
	int socketfd, new_fd;
	int rtn;
	pthread_t a_thread;
	socklen_t addrlen = (socklen_t) sizeof (struct sockaddr);

	/* Initiate the listening socket */
	socketfd = socket(AF_INET, SOCK_STREAM, 0);
	if (socketfd == -1) {
		perror("Error occured in socket\n");
		exit(1);
	}
	printf("[Server] Create socket: %d\n", socketfd);

	int tr = 1;

	if (setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &tr, sizeof (int)) ==
	    -1) {
		perror("setsockopt");
		exit(1);
	}

	bzero(&servip, sizeof (struct sockaddr_in));

	servip.sin_family = AF_INET;
	servip.sin_addr.s_addr = INADDR_ANY;
	servip.sin_port = htons(PORT);

	rtn = bind(socketfd, (const struct sockaddr *) &servip,
		 sizeof (struct sockaddr));
	if (rtn) {
		perror("Error occured in bind\n");
		exit(1);
	}

	/* Set up listening */
	rtn = listen(socketfd, MAX_PENDING_CONNECTIONS);
	if (rtn) {
		perror("Error occured in listen\n");
		exit(1);
	}

	while (1) {
		printf("[Server]Waiting for connection...\n");
		fflush(stdout);
		new_fd =
		    accept(socketfd, (struct sockaddr *) &clntip, &addrlen);
		if (new_fd == -1) {
			perror("Error occured in accept\n");
			continue;
		}
		printf("[Server] Client connected on socket %d...\n", new_fd);
		rtn = pthread_create(&a_thread, NULL, pthread_fn, &new_fd);
		if (rtn) {
			perror("Error occured in pthread_create\n");
			continue;
		}
		printf("[Server] create thread %d\n", rtn);
	}
	close(socketfd);
}

static void
__send_file(int sockfd, FILE * fp)
{
	char *buf = malloc(SEND_SIZE);
	size_t len, total_len = 0;
	while (1) {
		len = fread(buf, 1, SEND_SIZE, fp);
		if (len <= 0) {
			printf("[Thread] Total len %d sent\n", (int) total_len);
			goto out;
		}
		len = send(sockfd, buf, len, MSG_NOSIGNAL);
		total_len += len;
	}
      out:
	if (buf)
		free(buf);
}

static int
__recv_file(int sockfd, unsigned int size, FILE * fp)
{
	int r;
	int len, total_len = 0;
	char *buf = malloc(SEND_SIZE);

	while ((len = recv(sockfd, buf, SEND_SIZE, MSG_WAITALL)) > 0) {
		if (total_len + len > size)
			len = size - total_len;
		r = fwrite((const char *) buf, 1, len, fp);
		total_len += len;
		if (total_len >= size)
			break;
	}
	if (len < 0)
		printf("[Thread] Receive error %d\n", len);
	free(buf);
	if (total_len != size) {
		printf("[Thread] Received file size error: %d, %d\n", total_len,
		       (int) size);
		r = CLFS_ERROR;
	} else {
		printf("[Thread] Received file OK!\n");
		r = CLFS_OK;
	}
	return r;
}

void *
pthread_fn(void *arg)
{
	struct clfs_req req;
	int rtn;
	FILE *fp;
	int new_fd = *(int *) arg;
	char *path = malloc(30);

	rtn = recv(new_fd, &req, sizeof (struct clfs_req), 0);

	if (rtn != REQ_SIZE_32BIT) {
		send_status(new_fd, CLFS_ERROR);
		goto end_all;
	}

	sprintf(path, "clfs_store/%d.dat", req.inode);
	switch (req.type) {
	case CLFS_PUT:
		printf("[Thread] Receive PUT request\n");
		if (req.size > 0) {
			fp = fopen((const char *) path, "w");
			if (fp == NULL) {
				perror("[Thread] Can't access file\n");
				send_status(new_fd, CLFS_ACCESS);
				break;
			}
			send_status(new_fd, CLFS_OK);
			rtn = __recv_file(new_fd, req.size, fp);
			send_status(new_fd, rtn);
		}
		break;
	case CLFS_GET:
		printf("[Thread] Receive GET request\n");
		fp = fopen((const char *) path, "r");
		if (fp == NULL) {
			perror("[Thread] Can't access file\n");
			send_status(new_fd, CLFS_ACCESS);
			break;
		}
		send_status(new_fd, CLFS_OK);
		__send_file(new_fd, fp);
		rtn = read_status(new_fd);
		if (rtn == CLFS_OK) {
			/* delete file on server? */
			printf("[Thread] Send file success!\n");
			/* File should be deleted, but we leave it
			   here for grading purpose. Uncomment it
			   to do the delete */
			/* unlink((const char *) path); */
		}
		break;
	case CLFS_RM:
		printf("[Thread] Receive RM request\n");
		unlink((const char*) path);
		break;
	}

      end_all:
	if (fp)
		fclose(fp);
	free(path);
	close(new_fd);
	printf("[Thread] about to exit\n");
	pthread_exit(NULL);
}

void
send_status(int new_fd, enum clfs_status status)
{
	send(new_fd, &status, sizeof (status), MSG_NOSIGNAL);
	switch (status) {
	case CLFS_OK:
		break;
	case CLFS_INVAL:
		perror("[Error] Invalid address\n");
		break;
	case CLFS_ACCESS:
		perror("[Error] Couldn't read/write (length does not match)\n");
		break;
	case CLFS_ERROR:
		perror("[Error] Error occured in communicating\n");
		break;
	default:
		perror("Fine!\n");
	}
}

enum clfs_status
read_status(int fd)
{
	enum clfs_status s;
	int len;
	len = recv(fd, &s, sizeof (enum clfs_status), 0);
	if (len == sizeof (enum clfs_status))
		return s;
	printf("[Error] Something wrong in read_status\n");
	return CLFS_ERROR;
}
