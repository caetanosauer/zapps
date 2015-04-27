#include "basethread.h"

#include <chkpt.h>
#include <sm.h>
#include <restart.h>
#include <vol.h>

#ifndef USE_SHORE
#include <log_lsn_tracker.h>
#include <log_core.h>
#include <log_carray.h>
#endif

#ifndef USE_SHORE
sm_options basethread_t::_options;
#endif

basethread_t::basethread_t()
    : smthread_t(t_regular, "loginspect"), finished(false),
    current_xct(NULL)
{
    DO_PTHREAD(pthread_mutex_init(&running_mutex, NULL));
}

basethread_t::~basethread_t()
{
    DO_PTHREAD(pthread_mutex_destroy(&running_mutex));
}

void basethread_t::before_run()
{
    DO_PTHREAD(pthread_mutex_lock(&running_mutex));
    while (!to_mount.empty()) {
        mount_device(to_mount.front().c_str());
        to_mount.pop();
    }
}

void basethread_t::after_run()
{
    DO_PTHREAD(pthread_mutex_unlock(&running_mutex));
}

void basethread_t::queue_for_mount(string path)
{
    if (pthread_mutex_trylock(&running_mutex) == 0)
    {
        to_mount.push(path);
        DO_PTHREAD(pthread_mutex_unlock(&running_mutex));
    }
    else {
        throw runtime_error("Cannot mount device while thread is running");
    }
}

void basethread_t::start_base()
{
    sthread_t::initialize_sthreads_package();
    smthread_t::init_fingerprint_map();
#ifdef USE_SHORE
    smlevel_0::init_errorcodes();
#endif
    smlevel_0::errlog = new ErrLog("loginspect", log_to_stderr, "-");
}

void basethread_t::start_buffer()
{
    if (!smlevel_0::bf) {
#ifdef USE_SHORE
        // allocate memory for the buffer pool
        cerr << "Allocating buffer pool memory for " << npages
            << " pages ... ";
        char* shmbase; 
        long space_needed = bf_m::mem_needed(npages);
        W_COERCE(smthread_t::set_bufsize(space_needed, shmbase));
        cerr << "OK" << endl;

        // initialize buffer manager
        cerr << "Initializing buffer manager ... ";
        int4_t npgwriters = 1;
        const char* cleaner_policy = "normal";
        smlevel_0::bf = new bf_m(npages, shmbase, npgwriters, cleaner_policy);
        assert(smlevel_0::bf);
        shmbase +=  bf_m::mem_needed(npages);
        cerr << "OK" << endl;
#else
        smlevel_0::bf = new bf_tree_m(_options);

        assert(smlevel_0::bf);
#endif
    }
}

void basethread_t::start_io()
{
    if (!smlevel_0::io) {
        cerr << "Initializing I/O, device, and dir manager ... ";
        smlevel_0::io = new io_m;

#ifdef USE_SHORE
        bool log_page_flushes = false;
        io_m::set_log_page_flushes(log_page_flushes);
        assert(smlevel_0::io);

        smlevel_3::dir = new dir_m;
        assert(smlevel_3::dir);
#endif

    }
}

void basethread_t::start_log(string logdir)
{
    if (!smlevel_0::log) {
        // instantiate log manager
        log_m* log;
        cerr << "Initializing log manager ... " << flush;
#ifdef USE_SHORE
        const int logbufsize = 81920 * 1024; // 80 MB
        W_COERCE(log_m::new_log_m(log, logdir, logbufsize, false));
#else
        _options.set_string_option("sm_logidr", logdir);
        log = new log_core(_options);
#endif
        smlevel_0::log = log;
        cerr << "OK" << endl;
    }
}

void basethread_t::start_archiver(string archdir, size_t wsize, size_t bsize)
{
    LogArchiver* logArchiver;

    cerr << "Initializing log archiver ... " << flush;
#ifdef USE_SHORE
    const bool sort = true;
    W_COERCE(LogArchiver::constructOnce(logArchiver, archdir, sort, wsize));
#else
    _options.set_string_option("sm_archdir", archdir);
    _options.set_int_option("sm_archiver_workspace_size", wsize);
    _options.set_int_option("sm_archiver_block_size", bsize);
    logArchiver = new LogArchiver(_options);
#endif
    cerr << "OK" << endl;

    smlevel_0::logArchiver = logArchiver;
}

void basethread_t::start_merger(string /*archdir*/)
{
    //ArchiveMerger* archiveMerger;

    //cerr << "Initializing archive merger ... " << flush;
    //W_COERCE(
            //ArchiveMerger::constructOnce(archiveMerger, archdir, 1000,
                //1024 * 1024));
    //cerr << "OK" << endl;

    //smlevel_0::archiveMerger = archiveMerger;
}

void basethread_t::start_other()
{
    if (!smlevel_0::lm) {
        cerr << "Initializing lock manager ... ";
#ifdef USE_SHORE
        long locktablesize = 64000;
        smlevel_0::lm = new lock_m(locktablesize);
#else
        smlevel_0::lm = new lock_m(_options);
#endif
        cerr << "OK" << endl;

        cerr << "Initializing checkpoint manager ... ";
#ifdef USE_SHORE
        int4_t cp_flush_int = -1; 
        smlevel_1::chkpt = new chkpt_m(cp_flush_int);
        chkpt_m::preventive_mode = false;
#else
        smlevel_1::chkpt = new chkpt_m();
#endif
        cerr << "OK" << endl;

        cerr << "Initializing other system services ... " << flush;
        smlevel_2::bt = new btree_m;
#ifdef USE_SHORE
        smlevel_2::fi = new file_m;
        smlevel_2::rt = new rtree_m;
        smlevel_2::ra = new ranges_m;
#endif

        assert(
                smlevel_0::lm &&
                smlevel_1::chkpt &&
                smlevel_2::bt
              );

        cerr << "OK" << endl;
    }
}

 void basethread_t::print_stats()
{
    sm_stats_info_t stats;
    ss_m::gather_stats(stats);
    cout << stats << flushl;
}

 void basethread_t::mount_device(string path)
{
    io_m* io = smlevel_0::io;
#ifdef USE_SHORE
    dir_m* dir = smlevel_3::dir;
    assert(io && dir);
#else
    assert(io);
#endif

    size_t npages = 1024;
    
    W_COERCE(vol_t::format_dev(path.c_str(), npages, false));
    lvid_t lvid;
    vid_t tmp_vid;
    W_COERCE(io->get_new_vid(tmp_vid));
    W_COERCE(vol_t::format_vol(path.c_str(), lvid, tmp_vid, npages, true));
    W_COERCE(io->mount(path.c_str(), tmp_vid));

    // TODO: handle vid and multiple devices

    // CS: checkpoint must be taken to record the volume mount, otherwise
    // recovery will fail (TODO this general problem should be fixed)
    smlevel_4::chkpt->synch_take();

#ifdef USE_SHORE
    W_COERCE(dir->mount(path, vid));
#endif
}

/*
 * WARNING: if the transaction produces any log record (i.e., makes any
 * modification), then log_m must be started.
 */
void basethread_t::begin_xct()
{
    assert(current_xct == NULL);
    timeout_in_ms timeout = WAIT_SPECIFIED_BY_THREAD;
#ifdef USE_SHORE
    current_xct = xct_t::new_xct(0, timeout);
#else
    current_xct = new xct_t(NULL, timeout, false, false, false);
    smlevel_0::log->get_oldest_lsn_tracker()
        ->enter(reinterpret_cast<uintptr_t>(current_xct),
                smlevel_0::log->curr_lsn());
#endif
}

void basethread_t::commit_xct()
{
    assert(current_xct != NULL);
    current_xct->commit(false, NULL);
#ifdef USE_SHORE
    xct_t::destroy_xct(current_xct);
#else
    delete current_xct;
#endif
}
