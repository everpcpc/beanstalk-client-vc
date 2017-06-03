#pragma once

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

#include <WinSock2.h>
#include <WS2tcpip.h>
#include <string>
#include <stdexcept>
#include <vector>
#include <map>

#define BS_MAJOR_VERSION  1
#define BS_MINOR_VERSION  3
#define BS_PATCH_VERSION  0

#define BS_STATUS_OK             0
#define BS_STATUS_FAIL          -1
#define BS_STATUS_EXPECTED_CRLF -2
#define BS_STATUS_JOB_TOO_BIG   -3
#define BS_STATUS_DRAINING      -4
#define BS_STATUS_TIMED_OUT     -5
#define BS_STATUS_NOT_FOUND     -6
#define BS_STATUS_DEADLINE_SOON -7
#define BS_STATUS_BURIED        -8
#define BS_STATUS_NOT_IGNORED   -9


typedef struct bs_message {
	char *data;
	char *status;
	size_t size;
} BSM;

typedef struct bs_job {
	int64_t id;
	char *data;
	size_t size;
} BSJ;


// optional polling call, returns 1 if the socket is ready of the rw operation specified.
// rw: 1 => read, 2 => write, 3 => read/write
// fd: file descriptor of the socket
typedef int(*bs_poll_function)(int rw, SOCKET sock);

// export version
void bs_version(int *major, int *minor, int *patch);

// polling setup
void bs_start_polling(bs_poll_function f);
void bs_reset_polling(void);

// returns a descriptive text of the error code.
const char* bs_status_text(int code);

void bs_free_message(BSM* m);
void bs_free_job(BSJ *job);

// returns socket descriptor or BS_STATUS_FAIL
int bs_connect(const char *host, int port);
int bs_connect_with_timeout(const char *host, int port, float secs);

// returns job id or one of the negative failure codes.
int64_t bs_put(SOCKET sock, uint32_t priority, uint32_t delay, uint32_t ttr, const char *data, size_t bytes);

// rest return BS_STATUS_OK or one of the failure codes.
int bs_disconnect(SOCKET sock);
int bs_use(SOCKET sock, const char *tube);
int bs_watch(SOCKET sock, const char *tube);
int bs_ignore(SOCKET sock, const char *tube);
int bs_delete(SOCKET sock, int64_t job);
int bs_reserve(SOCKET sock, BSJ **job);
int bs_reserve_with_timeout(SOCKET sock, uint32_t ttl, BSJ **job);
int bs_release(SOCKET sock, int64_t id, uint32_t priority, uint32_t delay);
int bs_bury(SOCKET sock, int64_t id, uint32_t priority);
int bs_touch(SOCKET sock, int64_t id);
int bs_peek(SOCKET sock, int64_t id, BSJ **job);
int bs_peek_ready(SOCKET sock, BSJ **job);
int bs_peek_delayed(SOCKET sock, BSJ **job);
int bs_peek_buried(SOCKET sock, BSJ **job);
int bs_kick(SOCKET sock, int bound);
int bs_list_tube_used(SOCKET sock, char **tube);
int bs_list_tubes(SOCKET sock, char **yaml);
int bs_list_tubes_watched(SOCKET sock, char **yaml);
int bs_stats(SOCKET sock, char **yaml);
int bs_stats_job(SOCKET sock, int64_t id, char **yaml);
int bs_stats_tube(SOCKET sock, const char *tube, char **yaml);




namespace Beanstalk {
  
    typedef std::vector<std::string> info_list_t;
    typedef std::map<std::string, std::string> info_hash_t;

    class Job {
        public:
            int64_t id() const;
            std::string& body();
            Job(int64_t id, const char* data, size_t size);
            Job(BSJ*);
            Job();
            operator bool() { return _id > 0; }
        protected:
            int64_t _id;
            std::string _body;
    };

    class ConnectException: public std::runtime_error {
      public:
        ConnectException(const std::string message): std::runtime_error(message) {}
        ConnectException(void): std::runtime_error("Unable to connect to Beanstalk server") {}
    };
    
    class Client {
        public:
            ~Client();
            Client();
            Client(const std::string& host, int port, float timeout_secs = 0);
            bool ping();
            bool use(const std::string& tube);
            bool watch(const std::string& tube);
            bool ignore(const std::string& tube);
            int64_t put(const std::string& body, uint32_t priority = 0, uint32_t delay = 0, uint32_t ttr = 60);
            int64_t put(const char *data, size_t bytes, uint32_t priority, uint32_t delay, uint32_t ttr);
            bool del(int64_t id);
            bool del(const Job &job);
            bool reserve(Job &job);
            bool reserve(Job &job, uint32_t timeout);
            bool release(const Job &job, uint32_t priority = 1, uint32_t delay = 0);
            bool release(int64_t id, uint32_t priority = 1, uint32_t delay = 0);
            bool bury(const Job &job, uint32_t priority = 1);
            bool bury(int64_t id, uint32_t priority = 1);
            bool touch(const Job &job);
            bool touch(int64_t id);
            bool peek(Job &job, int64_t id);
            bool peek_ready(Job &job);
            bool peek_delayed(Job &job);
            bool peek_buried(Job &job);
            bool kick(int bound);
            void connect(const std::string& host, int port, float timeout_secs = 0);
            void reconnect();
            bool disconnect();
            void version(int *major, int *minor, int *patch);
            bool is_connected();
            std::string list_tube_used();
            info_list_t list_tubes();
            info_list_t list_tubes_watched();
            info_hash_t stats();
            info_hash_t stats_job(int64_t id);
            info_hash_t stats_tube(const std::string& name);
        protected:
            float _timeout_secs;
            int _handle;
            int _port;
            std::string _host;
    };
}
