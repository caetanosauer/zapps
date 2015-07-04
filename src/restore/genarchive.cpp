#include "genarchive.h"

#include <fstream>

void GenArchive::setupOptions()
{
    // default value
    long m = 274877906944L; // 256GB

    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Directory containing the log to be archived")
        ("archdir,a", po::value<string>(&archdir)->required(),
            "Directory where the archive runs will be stored (must exist)")
        ("maxLogSize,m", po::value<long>(&maxLogSize)->default_value(m),
            "max_logsize parameter of Shore-MT (default should be fine)")
    ;
}

void GenArchive::run()
{
    // check if directory exists
    ifstream f(archdir);
    bool exists = f.good();
    f.close();

    if (!exists) {
        // TODO make command and handler exceptions
        throw runtime_error("Directory does not exist: " + archdir);
    }

    /*
     * TODO if we add boost filesystem, we can create the directory
     * and if it already exists, check if there are any files in it
     */

    const size_t blockSize = 8192;
    const size_t workspaceSize = 3 * blockSize;

    start_base();
    start_io();
    start_log(logdir);
    start_archiver(archdir, workspaceSize, blockSize);

    lsn_t durableLSN = smlevel_0::log->durable_lsn();
    cerr << "Activating log archiver until LSN " << durableLSN << endl;

    smlevel_0::logArchiver->fork();

    // wait for all log to be archived
    smlevel_0::logArchiver->activate(durableLSN, true /* wait */);

    // by sending another activation signal with blocking,
    // we wait for logarchiver to consume up to durableLSN
    smlevel_0::logArchiver->activate(durableLSN, true);

    smlevel_0::logArchiver->shutdown();
    smlevel_0::logArchiver->join();

    smlevel_0::operating_mode = smlevel_0::t_in_redo;
    smlevel_0::logging_enabled = false;
}
