#include "restore_cmd.h"

#include "shore_env.h"
#include "vol.h"

void RestoreCmd::setupOptions()
{
    KitsCommand::setupOptions();
    options.add_options()
        ("backup", po::value<string>(&opt_backup)->default_value(""),
            "Path on which to store backup file")
    ;
}

void RestoreCmd::archiveLog()
{
    // archive whole log
    smlevel_0::logArchiver->activate(smlevel_0::log->curr_lsn(), true);
    while (smlevel_0::logArchiver->getNextConsumedLSN() < smlevel_0::log->curr_lsn()) {
        usleep(1000);
    }
    smlevel_0::logArchiver->start_shutdown();
    smlevel_0::logArchiver->join();
}

void RestoreCmd::run()
{
    if (archdir.empty()) {
        throw runtime_error("Log Archive is required to perform restore. \
                Specify path to archive directory with -a");
    }

    // STEP 1 - load database and take backup
    init();
    shoreEnv->load();

    vid_t vid(1);
    vol_t* vol = smlevel_0::vol->get(vid);

    archiveLog();
    if (!opt_backup.empty()) {
        vol->take_backup(opt_backup);
    }

    // STEP 2 - run benchmark and fail device
    runBenchmark();
    // TODO: should be an option
    vol->mark_failed(true /* evict */);

    // STEP 3 - continue benchmark on restored data
    runBenchmark();

    finish();
}
