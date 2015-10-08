#include "logpagestats.h"

class LogPageStatsHandler : public Handler {
public:
    size_t pageCount;
    size_t logrecs;
    size_t volume;
    lpid_t currentPage;

    LogPageStatsHandler() : pageCount(0), logrecs(0), volume(0),
        currentPage(lpid_t::null)
    {}

    virtual void invoke(logrec_t& r)
    {
        lpid_t pid = r.pid();
        if (pid != currentPage) {
            dumpCurrent();

            logrecs = 0;
            volume = 0;
            currentPage = pid;
            pageCount++;
        }

        logrecs++;
        volume += r.length();
    }

    void dumpCurrent() {
        if (logrecs == 0) { return; }

        cout << "pid=" << currentPage
            << " count=" << logrecs
            << " volume=" << volume
            << endl;
    }

    virtual void finalize()
    {
        dumpCurrent();
        pageCount++;
        cout << "TOTAL_PAGES=" << pageCount << endl;
    };
};

void LogPageStats::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void LogPageStats::run()
{
    LogPageStatsHandler* h = new LogPageStatsHandler();
    BaseScanner* s = getScanner();

    s->type_handlers.resize(logrec_t::t_max_logrec);
    s->any_handlers.push_back(h);
    s->fork();
    s->join();

    delete s;
    delete h;
}

