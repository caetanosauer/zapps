#include "basethread.h"

#include <device.h>
#include <chkpt.h>
#include <sm.h>
#include <restart.h>

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

void basethread_t::start_buffer(int npages)
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
        ErrLog* errlog = smlevel_0::errlog;

        int32_t npgwriters = _options.get_int_option("sm_num_page_writers", 1);
        if(npgwriters < 0) {
            errlog->clog << fatal_prio << "ERROR: num page writers must be positive : "
                << npgwriters
                << flushl;
            W_FATAL(eCRASH);
        }
        if (npgwriters == 0) {
            npgwriters = 1;
        }

        int64_t cleaner_interval_millisec_min = 
            _options.get_int_option("sm_cleaner_interval_millisec_min", 1000);
        if (cleaner_interval_millisec_min <= 0) {
            cleaner_interval_millisec_min = 1000;
        }

        int64_t cleaner_interval_millisec_max =
            _options.get_int_option("sm_cleaner_interval_millisec_max", 256000);
        if (cleaner_interval_millisec_max <= 0) {
            cleaner_interval_millisec_max = 256000;
        }
        bool initially_enable_cleaners =
            _options.get_bool_option("sm_backgroundflush", true);
        bool bufferpool_swizzle =
            _options.get_bool_option("sm_bufferpool_swizzle", false);
        std::string bufferpool_replacement_policy = 
            _options.get_string_option("sm_bufferpool_replacement_policy", "clock");
        uint32_t cleaner_write_buffer_pages = (uint32_t) 
            _options.get_int_option("sm_cleaner_write_buffer_pages", 64);

        smlevel_0::bf = new bf_tree_m(npages,
                npgwriters,
                cleaner_interval_millisec_min,
                cleaner_interval_millisec_max,
                cleaner_write_buffer_pages,
                bufferpool_replacement_policy.c_str(),
                initially_enable_cleaners,
                bufferpool_swizzle
                );

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

        smlevel_0::dev = new device_m;
        assert(smlevel_0::dev);
        cerr << "OK" << endl;
    }
}

void basethread_t::start_log(char* logdir, long max_logsz)
{
    if (!smlevel_0::log) {
        // instantiate log manager
        log_m* log;
        cerr << "Initializing log manager ... " << flush;
#ifdef USE_SHORE
        const int logbufsize = 81920 * 1024; // 80 MB
        smlevel_0::max_logsz = max_logsz;

        W_COERCE(log_m::new_log_m(log, logdir, logbufsize, false));
#else
        ErrLog* errlog = smlevel_0::errlog;
        (void) max_logsz;

        std::string logimpl = _options.
        get_string_option("sm_log_impl", log_core::IMPL_NAME);
        uint64_t logbufsize = _options.get_int_option("sm_logbufsize",
                128 << 10); // at least 1024KB

        // pretty big limit -- really, the limit is imposed by the OS's
        // ability to read/write
        if (uint64_t(logbufsize) < (uint64_t) 4 * ss_m::page_sz) {
            errlog->clog << fatal_prio
                << "Log buf size (sm_logbufsize = " << (int)logbufsize
                << " ) is too small for pages of size "
                << unsigned(ss_m::page_sz) << " bytes."
                << flushl;
            errlog->clog << fatal_prio
                << "Need to hold at least 4 pages ( " << 4 * ss_m::page_sz
                << ")"
                << flushl;
            W_FATAL(eCRASH);
        }
        if (uint64_t(logbufsize) > uint64_t(max_int4)) {
            errlog->clog << fatal_prio
                << "Log buf size (sm_logbufsize = " << (int)logbufsize
                << " ) is too big: individual log files can't be large files yet."
                << flushl;
            W_FATAL(eCRASH);
        }
        log = new log_core(
                logdir,
                logbufsize,      // logbuf_segsize
                _options.get_bool_option("sm_reformat_log", false),
                _options.get_int_option("sm_carray_slots",
                    ConsolidationArray::DEFAULT_ACTIVE_SLOT_COUNT)
                );
#endif
        smlevel_0::log = log;
        cerr << "OK" << endl;
    }
}

void basethread_t::start_archiver(char* archdir, size_t wsize)
{
    LogArchiver* logArchiver;

    cerr << "Initializing log archiver ... " << flush;
#ifdef USE_SHORE
    const bool sort = true;
    W_COERCE(LogArchiver::constructOnce(logArchiver, archdir, sort, wsize));
#else
    _options.set_string_option("sm_archdir", archdir);
    _options.set_int_option("sm_archiver_workspace_size", wsize);
    logArchiver = new LogArchiver(_options);
#endif
    cerr << "OK" << endl;

    smlevel_0::logArchiver = logArchiver;
}

void basethread_t::start_merger(char* /*archdir*/)
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
        smlevel_4::lid = new lid_m();

        assert(
                smlevel_0::lm &&
                smlevel_1::chkpt &&
                smlevel_2::bt &&
                smlevel_2::fi &&
                smlevel_2::rt &&
                smlevel_2::ra &&
                smlevel_4::lid
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

 void basethread_t::mount_device(const char* path)
{
    io_m* io = smlevel_0::io;
#ifdef USE_SHORE
    dir_m* dir = smlevel_3::dir;
    assert(io && dir);
#else
    assert(io);
#endif

    vid_t vid;
    u_int vol_cnt;
    // inform device_m about the device
    W_COERCE(io->mount_dev(path, vol_cnt));
    if (vol_cnt == 0) return;

    // make sure volumes on the dev are not already mounted
    lvid_t lvid;
    W_COERCE(io->get_lvid(path, lvid));
    vid = io->get_vid(lvid);
    if (vid != vid_t::null) {
        // already mounted
        return;
    }

    W_COERCE(io->get_new_vid(vid));
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
