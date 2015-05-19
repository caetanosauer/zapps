#ifndef BASETHREAD_H
#define BASETHREAD_H

#include "zapps-config.h"

#include "sm_base.h"
#include "vol.h"
#include "logarchiver.h"
#include "xct.h"

#include <stdexcept>
#include <queue>

using namespace std;

class basethread_t : public smthread_t {
public:
    basethread_t();

    virtual ~basethread_t();

    bool finished;

    static void start_base();
    static void start_buffer();
    static void start_log(string logdir);
    // default archiver workspace size = 800MB
    static void start_archiver(string archdir, size_t wsize, size_t bsize);
    static void start_merger(string archdir);
    static void start_other();
    static void start_io();
    static void print_stats();

    void queue_for_mount(string path);

protected:
    static void mount_device(string path);
    void begin_xct();
    void commit_xct();
    virtual void before_run();
    virtual void after_run();

private:
    queue<string> to_mount;
    pthread_mutex_t running_mutex;
    xct_t* current_xct;

    static sm_options _options;
};

#endif
