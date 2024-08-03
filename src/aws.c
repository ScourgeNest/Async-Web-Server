// SPDX-License-Identifier: BSD-3-Clause

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/sendfile.h>
#include <sys/eventfd.h>
#include <libaio.h>
#include <errno.h>

#include "aws.h"
#include "utils/util.h"
#include "utils/debug.h"
#include "utils/sock_util.h"
#include "utils/w_epoll.h"

/* server socket file descriptor */
static int listenfd;

/* epoll file descriptor */
static int epollfd;

static int aws_on_path_cb(http_parser *p, const char *buf, size_t len)
{
	struct connection *conn = (struct connection *)p->data;

	memcpy(conn->request_path, buf, len);
	conn->request_path[len] = '\0';
	conn->have_path = 1;

	return 0;
}

static void connection_prepare_send_reply_header(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the reply header. */
	char Ok[MAX_LINE] = AWS_OK;

	conn->send_len = strlen(Ok);
	memcpy(conn->send_buffer, Ok, conn->send_len);

	conn->send_pos = 0;
	conn->state = STATE_SENDING_HEADER;
}

static void remove_epoll_and_connection(struct connection *conn)
{
	int rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);

	DIE(rc < 0, "w_epoll_remove_ptr");

	/* remove current connection */
	connection_remove(conn);
}


static void connection_prepare_send_404(struct connection *conn)
{
	/* TODO: Prepare the connection buffer to send the 404 header. */
	char Not_Found[MAX_LINE] = AWS_NOT_FOUND;

	conn->send_len = strlen(Not_Found);
	memcpy(conn->send_buffer, Not_Found, conn->send_len);

	conn->state = STATE_SENDING_404;
}

static enum resource_type connection_get_resource_type(struct connection *conn)
{
	/* TODO: Get resource type depending on request path/filename. Filename should
	 * point to the static or dynamic folder.
	 */

	if (strstr(conn->filename, AWS_REL_STATIC_FOLDER) != NULL)
		return RESOURCE_TYPE_STATIC;
	else if (strstr(conn->filename, AWS_REL_DYNAMIC_FOLDER) != NULL)
		return RESOURCE_TYPE_DYNAMIC;
	else if (conn->fd == -1)
		return RESOURCE_TYPE_NONE;

	return RESOURCE_TYPE_NONE;
}


struct connection *connection_create(int sockfd)
{
	/* TODO: Initialize connection structure on given socket. */
	struct connection *conn = malloc(sizeof(*conn));

	if (!conn) {
		ERR("malloc");
		exit(1);
	}

	conn->sockfd = sockfd;
	memset(conn->recv_buffer, 0, BUFSIZ);
	conn->recv_len = 0;
	memset(conn->send_buffer, 0, BUFSIZ);
	conn->send_len = 0;
	conn->send_pos = 0;
	conn->file_pos = 0;
	conn->file_sent = 0;
	conn->async_read_len = 0;
	conn->have_path = 0;
	conn->state = STATE_INITIAL;


	conn->file_pos = 0;
	conn->file_size = 0;
	conn->ctx = 0;

	return conn;
}

int connection_start_async_io(struct connection *conn)
{
	int rc;

	conn->piocb[0] = &conn->iocb;

	rc = io_setup(1, &conn->ctx);
	if (rc < 0) {
		ERR("io_setup");
		return -1;
	}

	io_prep_pread(conn->piocb[0], conn->fd, (void *)conn->async_read_len, BUFSIZ, 0);

	int num_control_blocks = io_submit(conn->ctx, 1, conn->piocb);

	if (num_control_blocks < 0) {
		ERR("io_submit");
		return -1;
	}

	conn->async_read_len += num_control_blocks;

	rc = connection_complete_async_io(conn);
	if (rc == -1) {
		ERR("connection_complete_async_io");
		return -1;
	}

	while (conn->file_pos < conn->file_size) {
		size_t bytes_to_send;

		if (conn->file_size - conn->file_pos <= BUFSIZ)
			bytes_to_send = conn->file_size - conn->file_pos;
		else
			bytes_to_send = BUFSIZ;

		ssize_t bytes_sent = sendfile(conn->sockfd, conn->fd, (off_t *)&conn->file_pos,
									  bytes_to_send);

		conn->file_sent += bytes_sent;
	}
	conn->state = STATE_DATA_SENT;

	return 0;
}

void connection_remove(struct connection *conn)
{
	/* TODO: Remove connection handler. */

	close(conn->sockfd);

	conn->state = STATE_CONNECTION_CLOSED;

	free(conn);
}

void handle_new_connection(void)
{
	/* TODO: Handle a new connection request on the server socket. */
	int rc;
	int sockfd;
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);

	/* TODO: Accept new connection. */
	sockfd = accept(listenfd, (struct sockaddr *)&client_addr, &client_len);
	if (sockfd == -1) {
		ERR("accept");
		exit(1);
	}

	/* TODO: Set socket to be non-blocking. */
	rc = fcntl(sockfd, F_GETFL, 0);
	if (rc == -1) {
		ERR("fcntl F_GETFL");
		exit(1);
	}

	if (fcntl(sockfd, F_SETFL, rc | O_NONBLOCK) == -1) {
		ERR("fcntl F_SETFL O_NONBLOCK");
		exit(1);
	}

	/* TODO: Instantiate new connection handler. */
	struct connection *conn = connection_create(sockfd);

	if (!conn) {
		ERR("connection_create");
		exit(1);
	}

	/* TODO: Add socket to epoll. */
	rc = w_epoll_add_ptr_in(epollfd, sockfd, conn);
}

void receive_data(struct connection *conn)
{
	/* TODO: Receive message on socket.
	 * Store message in recv_buffer in struct connection.
	 */
	ssize_t bytes_recv;
	char abuffer[64];
	int rc;

	rc = get_peer_address(conn->sockfd, abuffer, 64);
	if (rc < 0) {
		ERR("get_peer_address");
		remove_epoll_and_connection(conn);
	}

	bytes_recv = recv(conn->sockfd, conn->recv_buffer + conn->recv_len,
					 BUFSIZ - conn->recv_len, 0);
	if (bytes_recv < 0) {
		/* error in communication */
		ERR("recv");
		remove_epoll_and_connection(conn);
	}
	if (bytes_recv == 0)
		/* connection closed */
		remove_epoll_and_connection(conn);
	conn->recv_len += bytes_recv;

	/* check if all the bytes were received*/
	char *endptr = conn->recv_buffer + conn->recv_len;

	if (strcmp(endptr - 4, "\r\n\r\n") != 0) {
		conn->state = STATE_RECEIVING_DATA;
		return;
	}

	conn->state = STATE_REQUEST_RECEIVED;
}

int connection_open_file(struct connection *conn)
{
	memset(conn->filename, 0, sizeof(conn->filename));

	sprintf(conn->filename, "%s%s", AWS_DOCUMENT_ROOT, conn->request_path);
	conn->fd = open(conn->filename, O_RDONLY);

	return 0; // Deschidere fișier cu succes
}

int connection_complete_async_io(struct connection *conn)
{
	/* TODO: Complete asynchronous operation; operation returns successfully.
	 * Prepare socket for sending.
	 */
	int rc;

	struct io_event *events = malloc(conn->async_read_len * sizeof(struct io_event));

	if (!events) {
		ERR("malloc");
		return -1;
	}

	rc = io_getevents(conn->ctx, conn->async_read_len, conn->async_read_len, events, NULL);

	if (rc < 0)
		ERR("io_getevents");
	free(events);
	return 0;
}

int parse_header(struct connection *conn)
{
	/* TODO: Parse the HTTP header and extract the file path. */
	/* Use mostly null settings except for on_path callback. */

	http_parser_settings settings_on_path = {
		.on_message_begin = 0,
		.on_header_field = 0,
		.on_header_value = 0,
		.on_path = aws_on_path_cb,
		.on_url = 0,
		.on_fragment = 0,
		.on_query_string = 0,
		.on_body = 0,
		.on_headers_complete = 0,
		.on_message_complete = 0
	};

	http_parser_init(&conn->request_parser, HTTP_REQUEST);
	conn->request_parser.data = conn;

	size_t nparsed = http_parser_execute(&conn->request_parser, &settings_on_path, conn->recv_buffer, conn->recv_len);

	if (nparsed != conn->recv_len) {
		ERR("http_parser_execute");
		remove_epoll_and_connection(conn);
		return -1;
	}
	return 0;
}

int connection_send_static(struct connection *conn)
{
	/* TODO: Send static data using sendfile(2). */

	int rc;

	if (conn->res_type == RESOURCE_TYPE_STATIC) {
		rc = sendfile(conn->sockfd, conn->fd, (off_t *) &conn->file_pos,
					 conn->file_size - conn->file_pos <= BUFSIZ ? conn->file_size - conn->file_pos : BUFSIZ);
		DIE(rc < 0, "sendfile");
		conn->file_sent += rc;

		if (conn->file_size > conn->file_sent) {
			/*Still need to send data*/
			conn->state = STATE_SENDING_DATA;
			return 1;
		}
	}

	return 0;
}

int connection_send_data(struct connection *conn)
{
	/* May be used as a helper function. */
	/* TODO: Send as much data as possible from the connection send buffer.
	 * Returns the number of bytes sent or -1 if an error occurred
	 */

	int bytes_sent = 0;

	if (conn->send_pos < conn->send_len) {
		/* trying to send header to socket*/
		bytes_sent = send(conn->sockfd, conn->send_buffer + conn->send_pos,
							conn->send_len - conn->send_pos, 0);
		if (bytes_sent < 0) {
			/* error in communication */
			ERR("send");
			return -1;
		}
		conn->send_pos += bytes_sent;

		conn->state = STATE_SENDING_DATA;
		return bytes_sent;
	}

	return bytes_sent;
}

int connection_send_dynamic(struct connection *conn)
{
	//check if the file was sent
	if (conn->file_size == conn->file_sent) {
		conn->state = STATE_DATA_SENT;
		return 0;
	}

	//check if the header was sent
	if (conn->send_pos < conn->send_len) {
		connection_send_data(conn);
		return 0;
	}

	if (connection_start_async_io(conn) == -1) {
		ERR("connection_start_async_io");
		remove_epoll_and_connection(conn);
		return -1;
	}

	return 0;
}


void handle_input(struct connection *conn)
{
	/* TODO: Handle input information: may be a new message or notification of
	 * completion of an asynchronous I/O operation.
	 */
	int rc;

	switch (conn->state) {
	case STATE_INITIAL:

		receive_data(conn);

		if (conn->state == STATE_CONNECTION_CLOSED || conn->state == STATE_RECEIVING_DATA)
			return;

		rc = parse_header(conn);
		if (rc == -1) {
			ERR("parse_header");
			remove_epoll_and_connection(conn);
			return;
		}

		rc = connection_open_file(conn);
		if (rc == -1) {
			ERR("connection_open_file");
			remove_epoll_and_connection(conn);
			return;
		}

		struct stat file_stat;

		fstat(conn->fd, &file_stat);
		conn->file_size = file_stat.st_size;

		if (conn->fd >= 0) {
			conn->state = STATE_SENDING_HEADER;
			connection_prepare_send_reply_header(conn);
		} else {
			conn->state = STATE_SENDING_404;
			connection_prepare_send_404(conn);
		}

		conn->res_type = connection_get_resource_type(conn);

		rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
		if (rc == -1) {
			ERR("w_epoll_update_ptr_out");
			remove_epoll_and_connection(conn);
			return;
		}
		break;
	case STATE_RECEIVING_DATA:

		receive_data(conn);

		if (conn->state == STATE_CONNECTION_CLOSED || conn->state == STATE_RECEIVING_DATA)
			return;

		rc = parse_header(conn);
		if (rc == -1) {
			ERR("parse_header");
			remove_epoll_and_connection(conn);
			return;
		}

		rc = connection_open_file(conn);
		if (rc == -1) {
			ERR("connection_open_file");
			remove_epoll_and_connection(conn);
			return;
		}

		struct stat file_stats;

		fstat(conn->fd, &file_stats);
		conn->file_size = file_stats.st_size;

		if (conn->fd >= 0) {
			conn->state = STATE_SENDING_HEADER;
			connection_prepare_send_reply_header(conn);
		} else {
			conn->state = STATE_SENDING_404;
			connection_prepare_send_404(conn);
		}

		conn->res_type = connection_get_resource_type(conn);

		rc = w_epoll_update_ptr_out(epollfd, conn->sockfd, conn);
		if (rc == -1) {
			ERR("w_epoll_update_ptr_out");
			remove_epoll_and_connection(conn);
			return;
		}
		break;
	default:
		return;
	}
}

void handle_output(struct connection *conn)
{
	/* TODO: Handle output information: may be a new valid requests or notification of
	 * completion of an asynchronous I/O operation or invalid requests.
	 */
	ssize_t bytes_sent;
	int rc;

	switch (conn->state) {
	case STATE_SENDING_HEADER:

		bytes_sent = connection_send_data(conn);
		if (bytes_sent != 0)
			return;
		/* remove out notification of the socket from epoll*/
		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_update_ptr_in");

		conn->state = STATE_DATA_SENT;
		break;

	case STATE_SENDING_DATA:

		bytes_sent = connection_send_data(conn);
		if (bytes_sent != 0)
			return;

		/* Send file until all bytes are sent*/
		rc = connection_send_static(conn);
		if (rc == 1)
			return;

		if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			rc = connection_send_dynamic(conn);
			if (rc == 1) {
				conn->state = STATE_ASYNC_ONGOING;
				return;
			}
		}
		/* remove out notification of the socket from epoll*/
		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_update_ptr_in");

		conn->state = STATE_DATA_SENT;
		break;

	case STATE_SENDING_404:

		bytes_sent = connection_send_data(conn);
		if (bytes_sent != 0)
			return;

		/* Send file until all bytes are sent*/
		rc = connection_send_static(conn);
		if (rc == 1)
			return;

		if (conn->res_type == RESOURCE_TYPE_DYNAMIC) {
			rc = connection_send_dynamic(conn);
			if (rc == 1) {
				conn->state = STATE_ASYNC_ONGOING;
				return;
			}
		}
		/* remove out notification of the socket from epoll*/
		rc = w_epoll_remove_ptr(epollfd, conn->sockfd, conn);
		DIE(rc < 0, "w_epoll_update_ptr_in");

		conn->state = STATE_DATA_SENT;
		break;
	default:
		return;
	}
}

void handle_client(uint32_t event, struct connection *conn)
{
	if (conn->state == STATE_CONNECTION_CLOSED)
		return;

	if (event & EPOLLIN)
		handle_input(conn);
	if (event & EPOLLOUT) {
		handle_output(conn);
		if (conn->state == STATE_DATA_SENT)
			connection_remove(conn);
	}

	if (event & EPOLLERR)
		connection_remove(conn);
}

int main(void)
{
	int rc;

	/* TODO: Initialize multiplexing. */
	epollfd = w_epoll_create();
	if (epollfd == -1) {
		ERR("epoll_create");
		exit(1);
	}

	/* TODO: Create server socket. */
	listenfd = tcp_create_listener(AWS_LISTEN_PORT, DEFAULT_LISTEN_BACKLOG);
	if (listenfd == -1) {
		ERR("tcp_create_listener");
		exit(1);
	}

	/* TODO: Add server socket to epoll object*/
	rc = w_epoll_add_fd_in(epollfd, listenfd);
	if (rc == -1) {
		ERR("epoll_ctl");
		exit(1);
	}

	/* Uncomment the following line for debugging. */
	// dlog(LOG_INFO, "Server waiting for connections on port %d\n", AWS_LISTEN_PORT);

	/* server main loop */
	while (1) {
		struct epoll_event rev;

		/* TODO: Wait for events. */
		rc = w_epoll_wait_infinite(epollfd, &rev);

		/* TODO: Switch event types; consider
		 *   - new connection requests (on server socket)
		 *   - socket communication (on connection sockets)
		 */
		switch (rev.data.fd) {
		case -1:
			ERR("epoll_wait");
			exit(1);
		default:
			if (rev.data.fd == listenfd)
				handle_new_connection();
			else
				handle_client(rev.events, (struct connection *)rev.data.ptr);
		}
	}

	return 0;
}
