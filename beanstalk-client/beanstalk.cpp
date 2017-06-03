#include "beanstalk.h"
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <errno.h>
#include <assert.h>
#include <cinttypes>

#pragma comment(lib, "WS2_32.lib")

using namespace std;

#define __STDC_FORMAT_MACROS

#define BS_STATUS_IS(message, code) strncmp(message, code, strlen(code)) == 0

#define BS_MESSAGE_NO_BODY  0
#define BS_MESSAGE_HAS_BODY 1

#ifndef BS_READ_CHUNK_SIZE
#define BS_READ_CHUNK_SIZE  4096
#endif

#define DATA_PENDING (errno == EAGAIN || errno == EWOULDBLOCK)

const char *bs_status_verbose[] = {
	"Success",
	"Operation failed",
	"Expected CRLF",
	"Job too big",
	"Queue draining",
	"Timed out",
	"Not found",
	"Deadline soon",
	"Buried",
	"Not ignored"
};

const char bs_resp_using[] = "USING";
const char bs_resp_watching[] = "WATCHING";
const char bs_resp_inserted[] = "INSERTED";
const char bs_resp_buried[] = "BURIED";
const char bs_resp_expected_crlf[] = "EXPECTED_CRLF";
const char bs_resp_job_too_big[] = "JOB_TOO_BIG";
const char bs_resp_draining[] = "DRAINING";
const char bs_resp_reserved[] = "RESERVED";
const char bs_resp_deadline_soon[] = "DEADLINE_SOON";
const char bs_resp_timed_out[] = "TIMED_OUT";
const char bs_resp_deleted[] = "DELETED";
const char bs_resp_not_found[] = "NOT_FOUND";
const char bs_resp_released[] = "RELEASED";
const char bs_resp_touched[] = "TOUCHED";
const char bs_resp_not_ignored[] = "NOT_IGNORED";
const char bs_resp_found[] = "FOUND";
const char bs_resp_kicked[] = "KICKED";
const char bs_resp_ok[] = "OK";

const char* bs_status_text(int code) {
	unsigned int cindex = (unsigned int)abs(code);
	return (cindex > sizeof(bs_status_verbose) / sizeof(char*)) ? 0 : bs_status_verbose[cindex];
}

int bs_resolve_address(const char *host, int port, struct sockaddr_in *server) {
	char service[64];
	struct addrinfo *addr, *rec;

	snprintf(service, 64, "%d", port);
	if (getaddrinfo(host, service, 0, &addr) != 0)
		return BS_STATUS_FAIL;

	for (rec = addr; rec != 0; rec = rec->ai_next) {
		if (rec->ai_family == AF_INET) {
			memcpy(server, rec->ai_addr, sizeof(*server));
			break;
		}
	}

	freeaddrinfo(addr);
	return BS_STATUS_OK;
}

int bs_connect(const char *host, int port) {
	int state;

	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 2), &wsaData);

	struct sockaddr_in server;

	SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock < 0 || bs_resolve_address(host, port, &server) < 0) {
		WSACleanup();
		return BS_STATUS_FAIL;
	}


	if (connect(sock, (struct sockaddr*)&server, sizeof(server)) != 0) {
		closesocket(sock);
		WSACleanup();
		return BS_STATUS_FAIL;
	}

	/* disable nagle - we buffer in the application layer */
	setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)(&state), sizeof(state));

	return sock;
}

int bs_connect_with_timeout(const char *host, int port, float secs) {
	int res, option, state = 1;
	socklen_t option_length;
	u_long imode = 0;

	WSADATA wsaData;
	WSAStartup(MAKEWORD(2, 2), &wsaData);

	SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

	struct sockaddr_in server;
	if (sock < 0 || bs_resolve_address(host, port, &server) < 0) {
		WSACleanup();
		return BS_STATUS_FAIL;
	}

	struct pollfd pfd;

	// Set non-blocking
	imode = 1;
	ioctlsocket(sock, FIONBIO, &imode);

	res = connect(sock, (struct sockaddr*)&server, sizeof(server));

	if (res < 0) {
		if (errno == EINPROGRESS) {
			// Init poll structure
			pfd.fd = sock;
			pfd.events = POLLOUT;

			if (WSAPoll(&pfd, 1, (int)(secs * 1000)) > 0) {
				option_length = sizeof(int);
				getsockopt(sock, SOL_SOCKET, SO_ERROR, (char*)(&option), &option_length);
				if (option) {
					closesocket(sock);
					WSACleanup();
					return BS_STATUS_FAIL;
				}
			}
			else {
				closesocket(sock);
				WSACleanup();
				return BS_STATUS_FAIL;
			}
		}
		else {
			closesocket(sock);
			WSACleanup();
			return BS_STATUS_FAIL;
		}
	}

	// Set to blocking mode
	imode = 0;
	ioctlsocket(sock, FIONBIO, &imode);

	/* disable nagle - we buffer in the application layer */
	setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char*)(&state), sizeof(state));

	return sock;
}

int bs_disconnect(SOCKET sock) {
	closesocket(sock);
	WSACleanup();
	return BS_STATUS_OK;
}

void bs_free_message(BSM* m) {
	if (m->status)
		free(m->status);
	if (m->data)
		free(m->data);
	free(m);
}

void bs_free_job(BSJ *job) {
	if (job->data)
		free(job->data);
	free(job);
}

// optional polling
bs_poll_function bs_poll = 0;

void bs_start_polling(bs_poll_function f) {
	bs_poll = f;
}

void bs_reset_polling() {
	bs_poll = 0;
}

BSM* bs_recv_message(SOCKET sock, int expect_data) {
	char *token, *data;
	size_t bytes, data_size, status_size, status_max = 512, expect_data_bytes = 0;
	SSIZE_T ret;
	BSM *message = (BSM*)calloc(1, sizeof(BSM));
	if (!message) return 0;

	message->status = (char*)calloc(1, status_max);
	if (!message->status) {
		bs_free_message(message);
		return 0;
	}

	// poll until ready to read
	if (bs_poll) bs_poll(1, sock);
	ret = recv(sock, message->status, status_max - 1, 0);
	if (ret < 0) {
		bs_free_message(message);
		return 0;
	}
	else {
		bytes = (size_t)ret;
	}

	token = strstr(message->status, "\r\n");
	if (!token) {
		bs_free_message(message);
		return 0;
	}

	*token = 0;
	status_size = (size_t)(token - message->status);

	if (expect_data) {
		token = strrchr(message->status, ' ');
		expect_data_bytes = token ? strtoul(token + 1, NULL, 10) : 0;
	}

	if (!expect_data || expect_data_bytes == 0)
		return message;

	message->size = bytes - status_size - 2;
	data_size = message->size > BS_READ_CHUNK_SIZE ? message->size + BS_READ_CHUNK_SIZE : BS_READ_CHUNK_SIZE;

	message->data = (char*)malloc(data_size);
	if (!message->data) {
		bs_free_message(message);
		return 0;
	}

	memcpy(message->data, message->status + status_size + 2, message->size);
	data = message->data + message->size;

	// already read the body along with status, all good.
	if ((expect_data_bytes + 2) <= message->size) {
		message->size = expect_data_bytes;
		return message;
	}

	while (1) {
		// poll until ready to read.
		if (bs_poll) bs_poll(1, sock);
		ret = recv(sock, data, data_size - message->size, 0);
		if (ret < 0) {
			if (bs_poll && DATA_PENDING)
				continue;
			else {
				bs_free_message(message);
				return 0;
			}
		}
		else {
			bytes = (size_t)ret;
		}

		// doneski, we have read enough bytes + \r\n
		if (message->size + bytes >= expect_data_bytes + 2) {
			message->size = expect_data_bytes;
			break;
		}

		data_size += BS_READ_CHUNK_SIZE;
		message->size += bytes;
		message->data = (char*)realloc(message->data, data_size);
		if (!message->data) {
			bs_free_message(message);
			return 0;
		}

		// move ahead pointer for reading more.
		data = message->data + message->size;
	}

	return message;
}

SSIZE_T bs_send_message(SOCKET sock, char *message, size_t size) {
	// poll until ready to write.
	if (bs_poll) bs_poll(2, sock);
	return send(sock, message, size, 0);
}

typedef struct bs_message_packet {
	char *data;
	size_t offset;
	size_t size;
} BSMP;

BSMP* bs_message_packet_new(size_t bytes) {
	BSMP *packet = (BSMP*)malloc(sizeof(BSMP));
	assert(packet);

	packet->data = (char*)malloc(bytes);
	assert(packet->data);

	packet->offset = 0;
	packet->size = bytes;

	return packet;
}

void bs_message_packet_append(BSMP *packet, const char *data, size_t bytes) {
	if (packet->offset + bytes > packet->size) {
		packet->data = (char*)realloc(packet->data, packet->size + bytes);
		assert(packet->data);
		packet->size += bytes;
	}

	memcpy(packet->data + packet->offset, data, bytes);
	packet->offset += bytes;
}

void bs_message_packet_free(BSMP *packet) {
	free(packet->data);
	free(packet);
}

#define BS_SEND(sock, command, size) {                        \
    if (bs_send_message(sock, command, size) < 0)             \
        return BS_STATUS_FAIL;                              \
}

#define BS_CHECK_MESSAGE(message) {                         \
    if (!message)                                           \
        return BS_STATUS_FAIL;                              \
}

#define BS_RETURN_OK_WHEN(message, okstatus) {              \
    if (BS_STATUS_IS(message->status, okstatus)) {          \
        bs_free_message(message);                           \
        return BS_STATUS_OK;                                \
    }                                                       \
}

#define BS_RETURN_FAIL_WHEN(message, nokstatus, nokcode) {  \
    if (BS_STATUS_IS(message->status, nokstatus)) {         \
        bs_free_message(message);                           \
        return nokcode;                                     \
    }                                                       \
}

#define BS_RETURN_INVALID(message) {                        \
    bs_free_message(message);                               \
    return BS_STATUS_FAIL;                                  \
}

int bs_use(SOCKET sock, const char *tube) {
	BSM *message;
	char command[1024] = { 0 };

	snprintf(command, 1024, "use %s\r\n", tube);
	BS_SEND(sock, command, strlen(command));

	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_using);
	BS_RETURN_INVALID(message);
}

int bs_watch(SOCKET sock, const char *tube) {
	BSM *message;
	char command[1024] = { 0 };

	snprintf(command, 1024, "watch %s\r\n", tube);
	BS_SEND(sock, command, strlen(command));

	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_watching);
	BS_RETURN_INVALID(message);
}

int bs_ignore(SOCKET sock, const char *tube) {
	BSM *message;
	char command[1024] = { 0 };

	snprintf(command, 1024, "ignore %s\r\n", tube);
	BS_SEND(sock, command, strlen(command));

	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_watching);
	BS_RETURN_INVALID(message);
}

int64_t bs_put(SOCKET sock, uint32_t priority, uint32_t delay, uint32_t ttr, const char *data, size_t bytes) {
	int64_t id;
	BSMP *packet;
	BSM *message;
	char command[1024] = { 0 };
	size_t command_bytes;

	snprintf(command, 1024, "put %" PRIu32 " %" PRIu32 " %" PRIu32 " %lu\r\n", priority, delay, ttr, bytes);

	command_bytes = strlen(command);
	packet = bs_message_packet_new(command_bytes + bytes + 3);
	bs_message_packet_append(packet, command, command_bytes);
	bs_message_packet_append(packet, data, bytes);
	bs_message_packet_append(packet, "\r\n", 2);

	// Can't use BS_SEND here, allocated memory needs to
	// be cleared on error
	int ret_code = bs_send_message(sock, packet->data, packet->offset);
	bs_message_packet_free(packet);
	if (ret_code < 0) {
		return BS_STATUS_FAIL;
	}

	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);

	if (BS_STATUS_IS(message->status, bs_resp_inserted)) {
		id = strtoll(message->status + strlen(bs_resp_inserted) + 1, NULL, 10);
		bs_free_message(message);
		return id;
	}

	if (BS_STATUS_IS(message->status, bs_resp_buried)) {
		id = strtoll(message->status + strlen(bs_resp_buried) + 1, NULL, 10);
		bs_free_message(message);
		return id;
	}

	BS_RETURN_FAIL_WHEN(message, bs_resp_expected_crlf, BS_STATUS_EXPECTED_CRLF);
	BS_RETURN_FAIL_WHEN(message, bs_resp_job_too_big, BS_STATUS_JOB_TOO_BIG);
	BS_RETURN_FAIL_WHEN(message, bs_resp_draining, BS_STATUS_DRAINING);
	BS_RETURN_INVALID(message);
}

int bs_delete(SOCKET sock, int64_t job) {
	BSM *message;
	char command[512] = { 0 };

	snprintf(command, 512, "delete %" PRId64 "\r\n", job);
	BS_SEND(sock, command, strlen(command));

	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_deleted);
	BS_RETURN_FAIL_WHEN(message, bs_resp_not_found, BS_STATUS_NOT_FOUND);
	BS_RETURN_INVALID(message);
}

int bs_reserve_job(SOCKET sock, char *command, BSJ **result) {
	BSJ *job;
	BSM *message;

	//  XXX: debug
	//  struct timeval start, end;
	//  gettimeofday(&start, 0);

	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_HAS_BODY);
	BS_CHECK_MESSAGE(message);

	if (BS_STATUS_IS(message->status, bs_resp_reserved)) {
		*result = job = (BSJ*)malloc(sizeof(BSJ));
		if (!job) {
			bs_free_message(message);
			return BS_STATUS_FAIL;
		}

		sscanf_s(message->status + strlen(bs_resp_reserved) + 1, "%" PRId64 " %lu", &job->id, &job->size);
		job->data = message->data;
		message->data = 0;
		bs_free_message(message);

		//  XXX: debug
		//  gettimeofday(&end, 0);
		//  printf("elapsed: %lu\n", (end.tv_sec * 1000000 + end.tv_usec) - (start.tv_sec * 1000000 + start.tv_usec));
		return BS_STATUS_OK;
	}

	// i don't think we'll ever hit this status code here.
	BS_RETURN_FAIL_WHEN(message, bs_resp_timed_out, BS_STATUS_TIMED_OUT);
	BS_RETURN_FAIL_WHEN(message, bs_resp_deadline_soon, BS_STATUS_DEADLINE_SOON);
	BS_RETURN_INVALID(message);
}

int bs_reserve(SOCKET sock, BSJ **result) {
	char *command = "reserve\r\n";
	return bs_reserve_job(sock, command, result);
}

int bs_reserve_with_timeout(SOCKET sock, uint32_t ttl, BSJ **result) {
	char command[512] = { 0 };
	snprintf(command, 512, "reserve-with-timeout %" PRIu32 "\r\n", ttl);
	return bs_reserve_job(sock, command, result);
}

int bs_release(SOCKET sock, int64_t id, uint32_t priority, uint32_t delay) {
	BSM *message;
	char command[512] = { 0 };

	snprintf(command, 512, "release %" PRId64 " %" PRIu32 " %" PRIu32 "\r\n", id, priority, delay);
	BS_SEND(sock, command, strlen(command));

	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_released);
	BS_RETURN_FAIL_WHEN(message, bs_resp_buried, BS_STATUS_BURIED);
	BS_RETURN_FAIL_WHEN(message, bs_resp_not_found, BS_STATUS_NOT_FOUND);
	BS_RETURN_INVALID(message);
}

int bs_bury(SOCKET sock, int64_t id, uint32_t priority) {
	BSM *message;
	char command[512] = { 0 };

	snprintf(command, 512, "bury %" PRId64 " %" PRIu32 "\r\n", id, priority);
	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_buried);
	BS_RETURN_FAIL_WHEN(message, bs_resp_not_found, BS_STATUS_NOT_FOUND);
	BS_RETURN_INVALID(message);
}

int bs_touch(SOCKET sock, int64_t id) {
	BSM *message;
	char command[512] = { 0 };

	snprintf(command, 512, "touch %" PRId64 "\r\n", id);
	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_touched);
	BS_RETURN_FAIL_WHEN(message, bs_resp_not_found, BS_STATUS_NOT_FOUND);
	BS_RETURN_INVALID(message);
}

int bs_peek_job(SOCKET sock, char *command, BSJ **result) {
	BSJ *job;
	BSM *message;

	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_HAS_BODY);
	BS_CHECK_MESSAGE(message);

	if (BS_STATUS_IS(message->status, bs_resp_found)) {
		*result = job = (BSJ*)malloc(sizeof(BSJ));
		if (!job) {
			bs_free_message(message);
			return BS_STATUS_FAIL;
		}

		sscanf_s(message->status + strlen(bs_resp_found) + 1, "%" PRId64 " %lu", &job->id, &job->size);
		job->data = message->data;
		message->data = 0;
		bs_free_message(message);

		return BS_STATUS_OK;
	}

	BS_RETURN_FAIL_WHEN(message, bs_resp_not_found, BS_STATUS_NOT_FOUND);
	BS_RETURN_INVALID(message);
}

int bs_peek(SOCKET sock, int64_t id, BSJ **job) {
	char command[512] = { 0 };
	snprintf(command, 512, "peek %" PRId64 "\r\n", id);
	return bs_peek_job(sock, command, job);
}

int bs_peek_ready(SOCKET sock, BSJ **job) {
	return bs_peek_job(sock, "peek-ready\r\n", job);
}

int bs_peek_delayed(SOCKET sock, BSJ **job) {
	return bs_peek_job(sock, "peek-delayed\r\n", job);
}

int bs_peek_buried(SOCKET sock, BSJ **job) {
	return bs_peek_job(sock, "peek-buried\r\n", job);
}

int bs_kick(SOCKET sock, int bound) {
	BSM *message;
	char command[512] = { 0 };

	snprintf(command, 512, "kick %d\r\n", bound);
	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	BS_CHECK_MESSAGE(message);
	BS_RETURN_OK_WHEN(message, bs_resp_kicked);
	BS_RETURN_INVALID(message);
}

int bs_list_tube_used(SOCKET sock, char **tube) {
	BSM *message;
	char command[64] = { 0 };
	snprintf(command, 64, "list-tube-used\r\n");
	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_NO_BODY);
	if (BS_STATUS_IS(message->status, bs_resp_using)) {
		*tube = (char*)calloc(1, strlen(message->status) - strlen(bs_resp_using) + 1);
		strcpy_s(*tube, strlen(message->status) - strlen(bs_resp_using) + 1, message->status + strlen(bs_resp_using) + 1);
		bs_free_message(message);
		return BS_STATUS_OK;
	}
	BS_RETURN_INVALID(message);
}

int bs_get_info(SOCKET sock, char *command, char **yaml) {
	BSM *message;
	size_t size;

	BS_SEND(sock, command, strlen(command));
	message = bs_recv_message(sock, BS_MESSAGE_HAS_BODY);
	BS_CHECK_MESSAGE(message);
	if (BS_STATUS_IS(message->status, bs_resp_ok)) {
		sscanf_s(message->status + strlen(bs_resp_ok) + 1, "%lu", &size);
		*yaml = message->data;
		(*yaml)[size] = 0;
		message->data = 0;
		bs_free_message(message);
		return BS_STATUS_OK;
	}

	BS_RETURN_INVALID(message);
}

int bs_list_tubes(SOCKET sock, char **yaml) {
	char command[64] = { 0 };
	snprintf(command, 64, "list-tubes\r\n");
	return bs_get_info(sock, command, yaml);
}

int bs_list_tubes_watched(SOCKET sock, char **yaml) {
	char command[64] = { 0 };
	snprintf(command, 64, "list-tubes-watched\r\n");
	return bs_get_info(sock, command, yaml);
}

int bs_stats(SOCKET sock, char **yaml) {
	char command[64] = { 0 };
	snprintf(command, 64, "stats\r\n");
	return bs_get_info(sock, command, yaml);
}

int bs_stats_job(SOCKET sock, int64_t id, char **yaml) {
	char command[128] = { 0 };
	snprintf(command, 128, "stats-job %" PRId64 "\r\n", id);
	return bs_get_info(sock, command, yaml);
}

int bs_stats_tube(SOCKET sock, const char *tube, char **yaml) {
	char command[512] = { 0 };
	snprintf(command, 512, "stats-tube %s\r\n", tube);
	return bs_get_info(sock, command, yaml);
}

void bs_version(int *major, int *minor, int *patch) {
	*major = BS_MAJOR_VERSION;
	*minor = BS_MINOR_VERSION;
	*patch = BS_PATCH_VERSION;
}


namespace Beanstalk {

    Job::Job() {
        _id = 0;
    }

    Job::Job(int64_t id, const char *data, size_t size) {
        _body.assign(data, size);
        _id = id;
    }

    Job::Job(BSJ *job) {
        if (job) {
            _body.assign(job->data, job->size);
            _id = job->id;
            bs_free_job(job);
        }
        else {
            _id = 0;
        }
    }

    string& Job::body() {
        return _body;
    }

    int64_t Job::id() const {
        return _id;
    }

    /* start helpers */

    void parsedict(stringstream &stream, info_hash_t &dict) {
        string key, value;
        while(true) {
            stream >> key;
            if (stream.eof()) break;
            if (key[0] == '-') continue;
            stream >> value;
            key.erase(--key.end());
            dict[key] = value;
        }
    }

    void parselist(stringstream &stream, info_list_t &list) {
        string value;
        while(true) {
            stream >> value;
            if (stream.eof()) break;
            if (value[0] == '-') continue;
            list.push_back(value);
        }
    }

    /* end helpers */

    Client::~Client() {
        if (_handle > 0)
          bs_disconnect(_handle);
        _handle = -1;
    }

    Client::Client() {
        _handle       = -1;
        _host         = "";
        _port         = 0;
        _timeout_secs = 0;
    }

    Client::Client(const std::string& host, int port, float secs) {
        _handle       = -1;
        connect(host, port, secs);
    }

    void Client::connect(const std::string& host, int port, float secs) {
        if (_handle > 0)
            throw ConnectException("already connected to beanstalkd at " + _host);

        _host         = host;
        _port         = port;
        _timeout_secs = secs;

        _handle = secs > 0 ? bs_connect_with_timeout((char *)_host.c_str(), _port, secs) : bs_connect((char*)host.c_str(), port);
        if (_handle < 0)
            throw ConnectException("unable to connect to beanstalkd at " + _host);
    }

    bool Client::is_connected() {
      return _handle > 0;
    }

    bool Client::disconnect() {
        if (_handle > 0 && bs_disconnect(_handle) == BS_STATUS_OK) {
            _handle = -1;
            return true;
        }
        return false;
    }

    void Client::version(int *major, int *minor, int *patch) {
        bs_version(major, minor, patch);
    }

    void Client::reconnect() {
        disconnect();
        connect(_host, _port, _timeout_secs);
    }

    bool Client::use(const std::string& tube) {
        return bs_use(_handle, (const char*)tube.c_str()) == BS_STATUS_OK;
    }

    bool Client::watch(const std::string& tube) {
        return bs_watch(_handle, (const char*)tube.c_str()) == BS_STATUS_OK;
    }

    bool Client::ignore(const std::string& tube) {
        return bs_ignore(_handle, (const char*)tube.c_str()) == BS_STATUS_OK;
    }

    int64_t Client::put(const std::string& body, uint32_t priority, uint32_t delay, uint32_t ttr) {
        int64_t id = bs_put(_handle, priority, delay, ttr, (const char*)body.data(), body.size());
        return (id > 0 ? id : 0);
    }

    int64_t Client::put(const char *body, size_t bytes, uint32_t priority, uint32_t delay, uint32_t ttr) {
        int64_t id = bs_put(_handle, priority, delay, ttr, body, bytes);
        return (id > 0 ? id : 0);
    }

    bool Client::del(const Job &job) {
        int response_code = bs_delete(_handle, (int64_t)job.id());

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::del(int64_t id) {
        int response_code = bs_delete(_handle, id);
        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::reserve(Job &job) {
        BSJ *bsj;
        int response_code = bs_reserve(_handle, &bsj);

        if (response_code == BS_STATUS_OK) {
            job = bsj;
            return true;
        }
        else if (response_code == BS_STATUS_FAIL) {
          throw ConnectException();
        }

        return false;
    }

    bool Client::reserve(Job &job, uint32_t timeout) {
        BSJ *bsj;
        int response_code = bs_reserve_with_timeout(_handle, timeout, &bsj);

        if (response_code == BS_STATUS_OK) {
            job = bsj;
            return true;
        }
        else if (response_code == BS_STATUS_FAIL) {
          throw ConnectException();
        }
        return false;
    }

    bool Client::release(const Job &job, uint32_t priority, uint32_t delay) {
        int response_code = bs_release(_handle, (int64_t)job.id(), priority, delay);

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::release(int64_t id, uint32_t priority, uint32_t delay) {
        int response_code = bs_release(_handle, id, priority, delay);

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::bury(const Job &job, uint32_t priority) {
        int response_code = bs_bury(_handle, (int64_t)job.id(), priority);

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::bury(int64_t id, uint32_t priority) {
        int response_code = bs_bury(_handle, id, priority);

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::touch(const Job &job) {
        int response_code = bs_touch(_handle, (int64_t)job.id());

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::touch(int64_t id) {
        int response_code = bs_touch(_handle, id);

        if (response_code == BS_STATUS_FAIL)
          throw ConnectException();

        return response_code == BS_STATUS_OK;
    }

    bool Client::peek(Job &job, int64_t id) {
        BSJ *bsj;
        if (bs_peek(_handle, id, &bsj) == BS_STATUS_OK) {
            job = bsj;
            return true;
        }
        return false;
    }

    bool Client::peek_ready(Job &job) {
        BSJ *bsj;
        if (bs_peek_ready(_handle, &bsj) == BS_STATUS_OK) {
            job = bsj;
            return true;
        }
        return false;
    }

    bool Client::peek_delayed(Job &job) {
        BSJ *bsj;
        if (bs_peek_delayed(_handle, &bsj) == BS_STATUS_OK) {
            job = bsj;
            return true;
        }
        return false;
    }

    bool Client::peek_buried(Job &job) {
        BSJ *bsj;
        if (bs_peek_buried(_handle, &bsj) == BS_STATUS_OK) {
            job = bsj;
            return true;
        }
        return false;
    }

    bool Client::kick(int bound) {
        return bs_kick(_handle, bound) == BS_STATUS_OK;
    }

    string Client::list_tube_used() {
        char *name;
        string tube;
        if (bs_list_tube_used(_handle, &name) == BS_STATUS_OK) {
            tube.assign(name);
            free(name);
        }

        return tube;
    }

    info_list_t Client::list_tubes() {
        char *yaml, *data;
        info_list_t tubes;
        if (bs_list_tubes(_handle, &yaml) == BS_STATUS_OK) {
            if ((data = strstr(yaml, "---"))) {
                stringstream stream(data);
                parselist(stream, tubes);
            }
            free(yaml);
        }
        return tubes;
    }

    info_list_t Client::list_tubes_watched() {
        char *yaml, *data;
        info_list_t tubes;
        if (bs_list_tubes_watched(_handle, &yaml) == BS_STATUS_OK) {
            if ((data = strstr(yaml, "---"))) {
                stringstream stream(data);
                parselist(stream, tubes);
            }
            free(yaml);
        }
        return tubes;
    }

    info_hash_t Client::stats() {
        char *yaml, *data;
        info_hash_t stats;
        string key, value;
        if (bs_stats(_handle, &yaml) == BS_STATUS_OK) {
            if ((data = strstr(yaml, "---"))) {
                stringstream stream(data);
                parsedict(stream, stats);
            }
            free(yaml);
        }
        return stats;
    }

    info_hash_t Client::stats_job(int64_t id) {
        char *yaml, *data;
        info_hash_t stats;
        string key, value;
        if (bs_stats_job(_handle, id, &yaml) == BS_STATUS_OK) {
            if ((data = strstr(yaml, "---"))) {
                stringstream stream(data);
                parsedict(stream, stats);
            }
            free(yaml);
        }
        return stats;
    }

    info_hash_t Client::stats_tube(const std::string& name) {
        char *yaml, *data;
        info_hash_t stats;
        string key, value;
        if (bs_stats_tube(_handle, (const char*)name.c_str(), &yaml) == BS_STATUS_OK) {
            if ((data = strstr(yaml, "---"))) {
                stringstream stream(data);
                parsedict(stream, stats);
            }
            free(yaml);
        }
        return stats;
    }

    bool Client::ping() {
        char *yaml;
        if (bs_list_tubes(_handle, &yaml) == BS_STATUS_OK) {
            free(yaml);
            return true;
        }

        return false;
    }
}
