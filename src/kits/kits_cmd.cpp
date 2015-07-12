#include "kits_cmd.h"

#include <stdexcept>
#include <string>

#define BOOST_FILESYSTEM_NO_DEPRECATED
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;

#include "shore_env.h"
#include "tpcb/tpcb_env.h"
#include "tpcb/tpcb_client.h"

#include "util/stopwatch.h"
#include "envvar.h"

int MAX_THREADS = 1000;

void KitsCommand::setupOptions()
{
    options.add_options()
        ("benchmark,b", po::value<string>(&opt_benchmark)->required(),
            "Benchmark to execute. Possible values: tpcb, tpcc")
        // ("config,c", po::value<string>(&opt_conffile)->required(),
        //     "Path to configuration file")
        ("dbfile,d", po::value<string>(&opt_dbfile)->default_value("db"),
            "Path to database file (non-empty)")
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Directory containing log to be scanned")
        ("archdir,a", po::value<string>(&archdir)->default_value(""),
            "Directory in which to store the log archive")
        ("load", po::value<bool>(&opt_load)->default_value(false)
            ->implicit_value(true),
            "If set, log and archive folders are emptied, database files \
            and backups are deleted, and dataset is loaded from scratch")
        ("bufsize", po::value<int>(&opt_bufsize)->default_value(0),
            "Size of buffer pool in Kbytes")
        ("trxs", po::value<int>(&opt_num_trxs)->default_value(100),
            "Number of transactions to execute")
        ("threads,t", po::value<int>(&opt_num_threads)->default_value(4),
            "Number of threads to execute benchmark with")
        ("select_trx,s", po::value<int>(&opt_select_trx)->default_value(0),
            "Transaction code or mix identifier (0 = all trxs)")
        ("queried_sf,q", po::value<int>(&opt_queried_sf)->default_value(1),
            "Scale factor to which to restrict queries")
        ("spread", po::value<bool>(&opt_spread)->default_value(true)
            ->implicit_value(true),
            "Attach each worker thread to a fixed core for improved concurrency")
    ;
}

void KitsCommand::run()
{
    init();
    if (opt_load) {
        shoreEnv->load();
    }
    runBenchmark();
    finish();
}

void KitsCommand::init()
{
    // just TPC-B for now
    if (opt_benchmark == "tpcb") {
        initShoreEnv<tpcb::ShoreTPCBEnv>();
    }
    else {
        throw runtime_error("Unknown benchmark string");
    }
}

void KitsCommand::runBenchmark()
{
    // just TPC-B for now
    if (opt_benchmark == "tpcb") {
        runBenchmarkSpec<tpcb::baseline_tpcb_client_t, tpcb::ShoreTPCBEnv>();
    }
    else {
        throw runtime_error("Unknown benchmark string");
    }
}

template<class Client, class Environment>
void KitsCommand::runBenchmarkSpec()
{
    // reset starting cpu and wh id
    int current_prs_id = -1;
    int wh_id = 0;

    Client* testers[MAX_NUM_OF_THR];

    shoreEnv->reset_stats();

    // reset monitor stats
#ifdef HAVE_CPUMON
    _g_mon->cntr_reset();
#endif

    // set measurement state to measure - start counting everything
    TRACE(TRACE_ALWAYS, "begin measurement\n");
    shoreEnv->set_measure(MST_MEASURE);
    stopwatch_t timer;

    // kick-off checkpoint thread
    // checkpointer_t * chkpter = NULL;
    // if (shoreEnv->get_chkpt_freq() > 0) {
    //     TRACE(TRACE_ALWAYS, "Starting checkpoint thread\n");
    //     chkpter = new checkpointer_t(shoreEnv);
    //     chkpter->fork();
    // }

    if (opt_queried_sf <= 0) {
        opt_queried_sf = shoreEnv->get_sf();
    }

    // 1. create and fork client clients
    int trxsPerThread = opt_num_trxs / opt_num_threads;
    for (int i=0; i<opt_num_threads; i++) {
        // create & fork testing threads
        if (opt_spread) {
            wh_id = (i%(int)opt_queried_sf)+1;
        }

        testers[i] = new Client("client-" + std::to_string(i), i,
                (Environment*) shoreEnv,
                MT_NUM_OF_TRXS, opt_select_trx, trxsPerThread,
                current_prs_id /* cpu id -- see below */,
                wh_id, opt_queried_sf);
        assert (testers[i]);
        testers[i]->fork();

        // CS: 1st arg is binding type, which I don't know what it is for
        // It seems like it is a way to specify what the next CPU id is.
        // If BT_NONE is given, it simply returns -1
        // current_prs_id = next_cpu(BT_NONE, current_prs_id);
    }

    // 2. join the tester threads
    for (int i=0; i<opt_num_threads; i++) {
        testers[i]->join();
        if (testers[i]->rv()) {
            TRACE( TRACE_ALWAYS, "Error in testing...\n");
            TRACE( TRACE_ALWAYS, "Exiting...\n");
            assert (false);
        }
        delete (testers[i]);
    }

    double delay = timer.time();
    //xct_stats stats = shell_get_xct_stats();
#ifdef HAVE_CPUMON
    _g_mon->cntr_pause();
    unsigned long miochs = _g_mon->iochars()/MILLION;
    double usage = _g_mon->get_avg_usage(true);
#else
    unsigned long miochs = 0;
    double usage = 0;
#endif
    TRACE(TRACE_ALWAYS, "end measurement\n");
    shoreEnv->print_throughput(opt_queried_sf, opt_spread, opt_num_threads, delay,
            miochs, usage);
}

template<class Environment>
void KitsCommand::initShoreEnv()
{
    shoreEnv = new Environment();

    loadOptions(shoreEnv->get_opts());

    // Kits does not seem to respect the db-config set in the file.
    // It always uses ssb-1. In the old Kits code, thie config was
    // specified with the -c command line
    stringstream benchCfg;
    benchCfg << opt_benchmark << "-" << opt_queried_sf;
    envVar::instance()->setConfiguration(benchCfg.str());

    shoreEnv->set_sf(opt_queried_sf);
    shoreEnv->set_qf(opt_queried_sf);

    shoreEnv->init();

    if (opt_load) {
        // delete existing logs and db files
        ensureEmptyPath(logdir);
        if (!archdir.empty()) {
            ensureEmptyPath(archdir);
        }
        ensureEmptyPath(opt_dbfile);
    }

    shoreEnv->set_clobber(opt_load);
    shoreEnv->set_device(opt_dbfile);

    shoreEnv->start();
}

void KitsCommand::mkdirs(string path)
{
    // if directory does not exist, create it
    fs::path fspath(path);
    if (!fs::exists(fspath)) {
        fs::create_directories(fspath);
    }
    else {
        if (!fs::is_directory(fspath)) {
            throw runtime_error("Provided path is not a directory!");
        }
    }
}

/**
 * If called on directory, remove all its contents, leavin an empty directory
 * behind. Otherwise, simply delete the file.
 */
void KitsCommand::ensureEmptyPath(string path)
{
    fs::path fspath(path);
    if (!fs::exists(path)) {
        return;
    }

    if (!fs::is_empty(fspath)) {
        if (fs::is_directory(fspath)) {
            fs::remove_all(fspath);
            fs::create_directory(fspath);
            w_assert1(fs::is_empty(fspath));
        }
        else {
            fs::remove(path);
            w_assert1(!fs::exists(fspath));
        }
    }
}

void KitsCommand::loadOptions(sm_options& options)
{
    options.set_string_option("sm_logdir", logdir);
    mkdirs(logdir);

    if (!archdir.empty()) {
        options.set_bool_option("sm_archiving", true);
        options.set_string_option("sm_archdir", archdir);
        mkdirs(archdir);
    }

    if (opt_dbfile.empty()) {
        throw runtime_error("Option dbfile cannot be empty!");
    }

    // ticker always turned on
    options.set_bool_option("sm_ticker_enable", true);

    if (opt_bufsize <= 0) {
        // TODO: default size for buffer pool may depend on SF and benchmark
        opt_bufsize = 8192; // 8 MB
    }
    options.set_int_option("sm_bufpoolsize", opt_bufsize);
}

void KitsCommand::finish()
{
    // shoreEnv has stop() and close(), and close calls stop (confusing!)
    shoreEnv->close();
}
