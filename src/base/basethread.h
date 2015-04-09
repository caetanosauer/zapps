#ifndef BASETHREAD_H
#define BASETHREAD_H

#include "zapps-config.h"

#define SM_SOURCE
#include <sm_int_4.h>

#ifndef USE_SHORE
#include <sm_options.h>
#endif

#include <stdexcept>
#include <queue>

// to allow log size > 2GB
#define ARCH_LP64

using namespace std;

class basethread_t : public smthread_t {
public:
    basethread_t();

    virtual ~basethread_t();

    bool finished;

    static void start_base();
    static void start_buffer(int npages);
    static void start_log(char* logdir, long max_logsz);
    // default archiver workspace size = 800MB
    static void start_archiver(char* archdir, size_t wsize = 8192 * 1024 *10);
    static void start_merger(char* archdir);
    static void start_other();
    static void start_io();
    static void print_stats();

    void queue_for_mount(string path);
    
protected:
    static void mount_device(const char* path);
    void begin_xct();
    void commit_xct();
    virtual void before_run();
    virtual void after_run();

private:
    queue<string> to_mount;
    pthread_mutex_t running_mutex;
    xct_t* current_xct;

#ifndef USE_SHORE
    static sm_options _options;
#endif
};

#endif
