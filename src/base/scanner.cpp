#include "scanner.h"

#include <device.h>
#include <chkpt.h>
#include <sm.h>
#include <restart.h>
#include <vol.h>

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
    if (r.tid() != tid_t::null){
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

BlockScanner::BlockScanner(const char* logdir, size_t blockSize, bool archive,
        bitset<logrec_t::t_max_logrec>* filter)
    : logdir(logdir), blockSize(blockSize), archive(archive),
    pnum(-1),
    runBegin(lsn_t::null), runEnd(lsn_t::null), runCount(0), runsScanned(0)
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
    const char * PREFIX = archive ? LogArchiver::RUN_PREFIX : "log.";
    lsn_t minLSN = lsn_t::null;

    while (entry != NULL) {
        const char* fname = entry->d_name;
        if (strncmp(PREFIX, fname, strlen(PREFIX)) == 0) {
            if (archive) {
                lsn_t lsn =
                    LogArchiver::ArchiveDirectory::parseLSN(fname, false);
                if (minLSN == lsn_t::null || lsn < minLSN) {
                    runEnd =
                        LogArchiver::ArchiveDirectory::parseLSN(fname, true);
                    minLSN = lsn;
                }
                runCount++;
            }
            else {
                int p = atoi(fname + strlen(PREFIX));
                if (p < pnum) {
                    pnum = p;
                }
            }
        }
        entry = os_readdir(dir);
    }
    os_closedir(dir);

    if (minLSN != lsn_t::null) {
        runBegin = minLSN;
    }
}

string BlockScanner::getNextFile()
{
    stringstream fname;
    fname << logdir << "/";
    if (archive) {
        if (runBegin == lsn_t::null) {
            findFirstFile();
        }
        else {
            // set boundaries for next file
            runBegin = lsn_t::null;
            os_dir_t dir = os_opendir(logdir);
            assert(dir);
            os_dirent_t* entry = os_readdir(dir);
            const char * PREFIX = LogArchiver::RUN_PREFIX;
            while (entry != NULL) {
                const char* fname = entry->d_name;
                if (strncmp(PREFIX, fname, strlen(PREFIX)) == 0) {
                    lsn_t lsn =
                        LogArchiver::ArchiveDirectory::parseLSN(fname, false);
                    if (lsn == runEnd) {
                        runBegin = runEnd;
                        runEnd = LogArchiver::ArchiveDirectory::
                            parseLSN(fname, false);
                        break;
                    }
                }
                entry = os_readdir(dir);
            }
            os_closedir(dir);

            if (runBegin == lsn_t::null && runsScanned != runCount) {
                throw runtime_error("Hole found in run boundaries!");
            }
        }

        runsScanned++;
        fname << LogArchiver::RUN_PREFIX << runBegin << "-" << runEnd;
    }
    else {
        if (pnum < 0) {
            findFirstFile();
        }
        else {
            pnum++;
        }
        fname << "log." << pnum;
    }

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

        if (archive && runBegin == lsn_t::null) {
            // scan is over
            break;
        }
        
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

    if (!archive && pnum == firstPartition && bpos == 0) {
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
    // default to 10k pages = 80MB
    start_buffer(10000);
    mount_device(devicePath.c_str());
    io_m* io = smlevel_0::io;
    start_other();

    begin_xct();

    lvid_t lvid;
    W_COERCE(io->get_lvid(devicePath.c_str(), lvid));
    vid_t vid = io->get_vid(lvid);

    if (scanStores) {
#ifdef CFG_SHORE
        // Store management was redesigned on Zero. In Shore, the obsolete
        // directory manager (dir_m) was used. In Zero, it was replaced by
        // the simpler stnode_cache_t
        snum_t snum = includeSystemPages ? 0 : 3;
        snum_t lastSnum;
        W_COERCE(io->max_store_id_in_use(vid, lastSnum));

        lpid_t pid, lastPid;
        while (snum <= lastSnum) {
            stid_t stid = stid_t(vid, snum);
            // CS: sdesc does nothing so far, so commented out
            /*
            sdesc_t* sdesc;
            rc_t rc = smlevel_3::dir->access(stid, sdesc, EX);
            if(rc.is_error()) {
                if(rc.err_num() == smlevel_0::eBADSTID) {
                    snum++;
                    continue;
                }
                W_COERCE(rc);
            }

            handleStore(stid, sdesc);
            */
            handleStore(stid);

            W_COERCE(io->first_page(stid, pid));
            W_COERCE(io->last_page(stid, lastPid));

            while (pid != lastPid) {
                handlePage(pid);
                W_COERCE(io->next_page(pid));
            }
            handlePage(lastPid);
            snum++;
        }
#else
        vol_t* vol = io->get_volume(vid);
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
#endif
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

