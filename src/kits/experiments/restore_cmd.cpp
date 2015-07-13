#include "restore_cmd.h"

#include "shore_env.h"
#include "vol.h"

class FailureThread : public smthread_t
{
public:
    FailureThread(vid_t vid, unsigned delay, bool evict)
        : smthread_t(t_regular, "FailureThread"),
        vid(vid), delay(delay), evict(evict)
    {
    }

    virtual ~FailureThread() {}

    virtual void run()
    {
        ::sleep(delay);

        vol_t* vol = smlevel_0::vol->get(vid);
        w_assert0(vol);
        vol->mark_failed(evict);
    }

private:
    vid_t vid;
    unsigned delay;
    bool evict;
};

void RestoreCmd::setupOptions()
{
    KitsCommand::setupOptions();
    options.add_options()
        ("backup", po::value<string>(&opt_backup)->default_value(""),
            "Path on which to store backup file")
        ("segmentSize", po::value<unsigned>(&opt_segmentSize)
            ->default_value(1024),
            "Size of restore segment in number of pages")
        ("singlePass", po::value<bool>(&opt_singlePass)->default_value(false)
            ->implicit_value(true),
            "Whether to use single-pass restore scheduler from the start")
        ("instant", po::value<bool>(&opt_instant)->default_value(true)
            ->implicit_value(true),
            "Use instant restore (i.e., access data before restore is done)")
        ("evict", po::value<bool>(&opt_evict)->default_value(false)
            ->implicit_value(true),
            "Evict all pages from buffer pool when failure happens")
        ("failDelay", po::value<unsigned>(&opt_failDelay)->default_value(60),
            "Time to wait before marking the volume as failed")
        ("crash", po::value<bool>(&opt_crash)->default_value(false)
            ->implicit_value(true),
            "Simulate media failure together with system failure")
        ("crashDelay", po::value<int>(&opt_crashDelay)->default_value(0),
            "Number of seconds passed between media and system failure. \
            If <= 0, system comes back up with device failed, i.e., \
            volume is marked failed immediately after log analysis.")
        ("postRestoreWorkFactor", po::value<float>(&opt_postRestoreWorkFactor)
            ->default_value(5.0),
            "Numer of transactions to execute after media failure is the \
            number executed before failure times this factor")
        ("concurrentArchiving", po::value<bool>(&opt_concurrentArchiving)
            ->default_value(false)
            ->implicit_value(true),
            "Run log archiving concurrently with benchmark execution and \
            restore, instead of generating log archive \"offline\" when \
            marking the volume as failed")
        ("eager", po::value<bool>(&opt_eager)->default_value(true)
            ->implicit_value(true),
            "Run log archiving in eager mode")
        // further options to add:
        // fail volume again while it is being restored
        // fail and restore multiple times in a loop
        //
    ;
}

void RestoreCmd::archiveLog()
{
    // archive whole log
    smlevel_0::logArchiver->activate(smlevel_0::log->curr_lsn(), true);
    while (smlevel_0::logArchiver->getNextConsumedLSN() < smlevel_0::log->curr_lsn()) {
        usleep(1000);
    }
    smlevel_0::logArchiver->shutdown();
    smlevel_0::logArchiver->join();
}

void RestoreCmd::loadOptions(sm_options& options)
{
    KitsCommand::loadOptions(options);
    options.set_bool_option("sm_archiver_eager", opt_eager);
    options.set_int_option("sm_restore_segsize", opt_segmentSize);
    options.set_bool_option("sm_restore_instant", opt_instant);
}

void RestoreCmd::run()
{
    if (archdir.empty()) {
        throw runtime_error("Log Archive is required to perform restore. \
                Specify path to archive directory with -a");
    }

    // STEP 1 - load database and take backup
    if (opt_load) {
        // delete existing backups
        if (!opt_backup.empty()) {
            ensureEmptyPath(opt_backup);
        }
    }
    init();

    if (opt_load) {
        shoreEnv->load();
    }

    vid_t vid(1);
    vol_t* vol = smlevel_0::vol->get(vid);

    if (!opt_eager) {
        archiveLog();
    }

    if (!opt_backup.empty()) {
        vol->take_backup(opt_backup);
    }

    // STEP 2 - spawn failure thread and run benchmark
    FailureThread* t = new FailureThread(vid, opt_failDelay, opt_evict);
    t->fork();

    // TODO if crash is on, move runBenchmark into a separate thread
    // and crash after specified delay. To crash, look at the restart
    // test classes and shutdown the SM like it's done there. Then,
    // bring it back up again and call mark_failed after system comes
    // back. If instant restart is on, then REDO will invoke restore.
    // Meanwhile, the thread running the benchmark will accumulate
    // errors, which should be ok (see trx_worker_t::_serve_action).

    // opt_num_trxs *= opt_postRestoreWorkFactor;
    runBenchmark();

    t->join();
    delete t;

    finish();
}
