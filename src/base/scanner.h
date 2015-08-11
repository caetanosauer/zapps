#ifndef SCANNER_H
#define SCANNER_H


#include "sm_base.h"
#include "lsn.h"
#include "logarchiver.h"

#include "basethread.h"
#include "handler.h"

#include <bitset>
#include <functional>

class BaseScanner : public basethread_t {
public:
    BaseScanner() : restrictFile("")
    {}

    virtual ~BaseScanner()
    {} // TODO do we need to delete the handlers here?

    void setRestrictFile(string fname) { restrictFile = fname; }
protected:
    virtual void handle(logrec_t* lr);
    virtual void finalize();

public: // TODO make protected and add register methods
    std::vector<Handler*> any_handlers;
    std::vector<Handler*> pid_handlers;
    std::vector<Handler*> transaction_handlers;
    std::vector<std::vector<Handler*>> type_handlers;
    std::function<void(const char*)> openFileCallback;

protected:
    string restrictFile;
};

class ShoreScanner : public BaseScanner {
};

class BlockScanner : public BaseScanner {
public:
    BlockScanner(const char* logdir, size_t blockSize,
            bitset<logrec_t::t_max_logrec>* filter = NULL);
    virtual ~BlockScanner();

    virtual void run();
private:
    LogScanner* logScanner;
    char* currentBlock;
    const char* logdir;
    size_t blockSize;
    int pnum;

    void findFirstFile();
    string getNextFile();
};

class LogArchiveScanner : public BaseScanner {
public:
    LogArchiveScanner(string archdir);
    virtual ~LogArchiveScanner() {};

    virtual void run();
private:
    string archdir;
    lsn_t runBegin;
    lsn_t runEnd;

    void findFirstFile();
    string getNextFile();
};

class MergeScanner : public BaseScanner {
public:
    MergeScanner(string archdir);
    virtual ~MergeScanner() {};

    virtual void run();
private:
    string archdir;
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
