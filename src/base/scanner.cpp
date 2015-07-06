#include "scanner.h"

#include <chkpt.h>
#include <sm.h>
#include <restart.h>
#include <vol.h>

#define PARSE_LSN(a,b) \
    LogArchiver::ArchiveDirectory::parseLSN(a, b);


void BaseScanner::handle(logrec_t* lr)
{
    logrec_t& r = *lr;
    size_t i;
    for (i=0; i < any_handlers.size(); i++)
        any_handlers.at(i)->invoke(r);
    if (!r.null_pid()){
        for (i=0; i < pid_handlers.size(); i++)
            pid_handlers.at(i)->invoke(r);
    }
    if (r.is_single_sys_xct() || r.tid() != tid_t::null){
        for (i=0; i < transaction_handlers.size(); i++)
            transaction_handlers.at(i)->invoke(r);
    }
    for (i=0; type_handlers.size() > 0 && i < type_handlers.at(r.type()).size(); i++)
        type_handlers.at(r.type()).at(i)->invoke(r);
}

void BaseScanner::finalize()
{
    size_t i;
    for (i=0; i < any_handlers.size(); i++)
        any_handlers.at(i)->finalize();
    for (i=0; i < pid_handlers.size(); i++)
        pid_handlers.at(i)->finalize();
    for (i=0; i < transaction_handlers.size(); i++)
        transaction_handlers.at(i)->finalize();
    for (i=0; type_handlers.size() > 0 && i < logrec_t::t_max_logrec; i++)
    {
        for (size_t j=0; type_handlers.at(i).size() > 0 &&
                j < type_handlers.at(i).size(); j++)
        {
            type_handlers.at(i).at(j)->finalize();
        }
    }
}

BlockScanner::BlockScanner(const char* logdir, size_t blockSize,
        bitset<logrec_t::t_max_logrec>* filter)
    : logdir(logdir), blockSize(blockSize), pnum(-1)
{
    logScanner = new LogScanner(blockSize);
    currentBlock = new char[blockSize];

    if (filter) {
        logScanner->ignoreAll();
        for (int i = 0; i < logrec_t::t_max_logrec; i++) {
            if (filter->test(i)) {
                logScanner->unsetIgnore((logrec_t::kind_t) i);
            }
        }
    }
}

void BlockScanner::findFirstFile()
{
    pnum = numeric_limits<int>::max();
    os_dir_t dir = os_opendir(logdir);
    if (!dir) {
        smlevel_0::errlog->clog << fatal_prio <<
            "Error: could not open recovery log dir: " <<
            logdir << flushl;
        W_COERCE(RC(fcOS));
    }
    os_dirent_t* entry = os_readdir(dir);
    const char * PREFIX = "log.";

    while (entry != NULL) {
        const char* fname = entry->d_name;
        if (strncmp(PREFIX, fname, strlen(PREFIX)) == 0) {
            int p = atoi(fname + strlen(PREFIX));
            if (p < pnum) {
                pnum = p;
            }
        }
        entry = os_readdir(dir);
    }
    os_closedir(dir);
}

string BlockScanner::getNextFile()
{
    stringstream fname;
    fname << logdir << "/";
    if (pnum < 0) {
        findFirstFile();
    }
    else {
        pnum++;
    }
    fname << "log." << pnum;

    if (openFileCallback) {
        openFileCallback(fname.str().c_str());
    }

    return fname.str();
}

void BlockScanner::run()
{
    size_t bpos = 0;
    streampos fpos = 0, fend = 0;
    //long count = 0;
    int firstPartition = pnum;
    logrec_t* lr = NULL;

    while (true) {
        // open partition number pnum
        string fname = getNextFile();
        ifstream in(fname, ios::binary | ios::ate);

        // does the file exist?
        if (!in.good()) {
            in.close();
            break;
        }

        // file is opened at the end
        fend = in.tellg();
        fpos = 0;

        cerr << "Scanning log file " << fname << endl;

        while (fpos < fend) {
            //cerr << "Reading block at " << fpos << " from " << fname.str();

            // read next block from partition file
            in.seekg(fpos);
            if (in.fail()) {
                throw runtime_error("IO error seeking into file");
            }
            in.read(currentBlock, blockSize);
            if (in.eof()) {
                // partial read on end of file
                fpos = fend;
            }
            else if (in.fail()) {
                // EOF implies fail, so we check it first
                throw runtime_error("IO error reading block from file");
            }
            else {
                fpos += blockSize;
            }

            //cerr << " - " << in.gcount() << " bytes OK" << endl;

            bpos = 0;
            while (logScanner->nextLogrec(currentBlock, bpos, lr)) {
                handle(lr);
            }
        }

        in.close();
    }

    if (pnum == firstPartition && bpos == 0) {
        throw runtime_error("Could not find/open log files in "
                + string(logdir));
    }

    BaseScanner::finalize();
}

BlockScanner::~BlockScanner()
{
    delete currentBlock;
    delete logScanner;
}


LogArchiveScanner::LogArchiveScanner(string archdir)
    : archdir(archdir), runBegin(lsn_t::null), runEnd(lsn_t::null)
{
}

bool runCompare (string a, string b)
{
    lsn_t lsn_a = PARSE_LSN(a.c_str(), false);
    lsn_t lsn_b = PARSE_LSN(b.c_str(), false);
    return lsn_a < lsn_b;
}

void LogArchiveScanner::run()
{
    LogArchiver::ArchiveDirectory* directory = new
        // CS TODO -- fix block size bug (Issue #9)
        LogArchiver::ArchiveDirectory(archdir, 1024 * 1024);

    std::vector<std::string> runFiles;
    directory->listFiles(&runFiles);

    std::sort(runFiles.begin(), runFiles.end(), runCompare);

    runBegin = PARSE_LSN(runFiles[0].c_str(), false);
    runEnd = PARSE_LSN(runFiles[0].c_str(), true);
    std::vector<std::string>::const_iterator it;
    for(size_t i = 1; i < runFiles.size(); i++) {
        // begin of run i must be equal to end of run i-1
        runBegin = PARSE_LSN(runFiles[i].c_str(), false);
        if (runBegin != runEnd) {
            throw runtime_error("Hole found in run boundaries!");
        }
        runEnd = PARSE_LSN(runFiles[i].c_str(), true);

        LogArchiver::ArchiveScanner::RunScanner* rs =
            new LogArchiver::ArchiveScanner::RunScanner(
                    runBegin,
                    runEnd,
                    lpid_t::null, // first PID
                    lpid_t::null, // last PID
                    0,            // file offset
                    directory
            );

        logrec_t* lr;
        while (rs->next(lr)) {
            handle(lr);
        };

        delete lr;
    }
}

MergeScanner::MergeScanner()
{
    archiveMerger = smlevel_0::archiveMerger;
    if (!archiveMerger) {
        throw runtime_error("Archive merger was not initialized!");
    }
}

void MergeScanner::run()
{
    ArchiveMerger::MergeOutput* m = archiveMerger->offlineMerge(false);
    char* lrbuf = new char[sizeof(logrec_t)];

    //lsn_t prev_lsn = lsn_t::null;
    //lpid_t prev_pid = lpid_t::null;
    //ulong count = 0;

    while (m->copyNext(lrbuf)) {
        logrec_t* lr = (logrec_t*) lrbuf;
        handle(lr);
    }

    delete lrbuf;
    delete m;

    BaseScanner::finalize();
}

void PageScanner::run()
{
    start_base();
    start_io();
    start_buffer();
    mount_device(devicePath.c_str());
    vol_m* volmgr = smlevel_0::vol;
    start_other();

    begin_xct();

    if (scanStores) {
        vol_t* vol = volmgr->get(devicePath.c_str());
        vid_t vid = vol->vid();
        stnode_cache_t* scache = vol->get_stnode_cache();
        const std::vector<snum_t>& snums = scache->get_all_used_store_ID();

        lpid_t pid, lastPid;
        for (size_t i = 0; i < snums.size(); i++) {
            stid_t stid = stid_t(vid, snums[i]);
            handleStore(stid);

            /* CS: TODO -- this method of scanning the pages of
             * a store does not work on Zero, because the methods
             * first/next/last_page don't exist anymore. I suspect
             * it is only possible to do a breadth-first search from
             * the root B-tree page, which is given by
             * io_m::get_root(stid)
             */
            /*
            W_COERCE(io->first_page(stid, pid));
            W_COERCE(io->last_page(stid, lastPid));

            while (pid != lastPid) {
                handlePage(pid);
                W_COERCE(io->next_page(pid));
            }
            handlePage(lastPid);
            */
        }
    }
    else {
        // TODO?
    }

    finalize();
    commit_xct();
}

void PageScanner::handlePage(const lpid_t& pid)
{
    vector<PageHandler*>::iterator it = pageHandlers.begin();
    while (it != pageHandlers.end()) {
        (*it)->handle(pid);
        it++;
    }
}

void PageScanner::handleStore(const stid_t& stid)
{
    vector<StoreHandler*>::iterator it = storeHandlers.begin();
    while (it != storeHandlers.end()) {
        (*it)->handle(stid);
        it++;
    }
}

void PageScanner::finalize()
{
    {
        vector<StoreHandler*>::iterator it = storeHandlers.begin();
        while (it != storeHandlers.end()) {
            (*it)->finalize();
            it++;
        }
    }

    {
        vector<PageHandler*>::iterator it = pageHandlers.begin();
        while (it != pageHandlers.end()) {
            (*it)->finalize();
            it++;
        }
    }
}

