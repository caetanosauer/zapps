#ifndef SCANNER_H
#define SCANNER_H

#include "zapps-config.h"

#define SM_SOURCE
#include <sm_int_4.h>

#include "basethread.h"
#include "handler.h"

#include <bitset>

class BaseScanner : public basethread_t {
public:
    virtual ~BaseScanner()
    {}; // TODO do we need to delete the handlers here?
protected:
    virtual void handle(logrec_t* lr);
    virtual void finalize();
public: // TODO make protected and add register methods
    std::vector<handler*> any_handlers;
    std::vector<handler*> pid_handlers;
    std::vector<handler*> transaction_handlers;
    std::vector<std::vector<handler*>> type_handlers;
    std::function<void(const char*)> openFileCallback;
};

class ShoreScanner : public BaseScanner {
};

class BlockScanner : public BaseScanner {
public:
    BlockScanner(const char* logdir, size_t blockSize, bool archive = false,
            bitset<logrec_t::t_max_logrec>* filter = NULL);
    virtual ~BlockScanner();

    virtual void run();
private:
    LogScanner* logScanner;
    char* currentBlock;
    const char* logdir;
    size_t blockSize;
    bool archive;
    int pnum;
    lsn_t runBegin;
    lsn_t runEnd;
    size_t runCount;
    size_t runsScanned;

    void findFirstFile();
    string getNextFile();
};

class MergeScanner : public BaseScanner {
public:
    MergeScanner();
    virtual ~MergeScanner() {};

    virtual void run();
private:
    ArchiveMerger* archiveMerger;
};

/*
 * Scans all pages in the database one store at a time
 */
class PageScanner : public basethread_t {
public:
    PageScanner(string devicePath, bool inclSys = true, bool scanStores = true)
        : devicePath(devicePath), includeSystemPages(inclSys), 
        scanStores(scanStores) {}
    virtual ~PageScanner() {};

    virtual void run();
    void addPageHandler(PageHandler* h) { pageHandlers.push_back(h); }
    void addStoreHandler(StoreHandler* h) { storeHandlers.push_back(h); }
protected:
    virtual void handlePage(const lpid_t&);
    virtual void handleStore(const stid_t&);
    virtual void finalize();
private:
    string devicePath;
    bool includeSystemPages;
    bool scanStores;
    vector<PageHandler*> pageHandlers;
    vector<StoreHandler*> storeHandlers;
};

#endif
