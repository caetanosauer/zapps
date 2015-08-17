#include "verifylog.h"

#include <fstream>
#include "logarchiver.h"

void VerifyLog::setupOptions()
{
    LogScannerCommand::setupOptions();
}

void VerifyLog::run()
{
    BaseScanner* s = getScanner();
    VerifyHandler* h = new VerifyHandler(merge);
    if (!merge) {
        s->openFileCallback = std::bind(&VerifyHandler::newFile, h,
            std::placeholders::_1);
    }
    s->any_handlers.push_back(h);
    s->fork();
    s->join();

    delete h;
    delete s;
}

VerifyHandler::VerifyHandler(bool merge)
    : minLSN(lsn_t::null), maxLSN(lsn_t::null), lastLSN(lsn_t::null),
    lastPID(lpid_t::null), count(0), merge(merge)
{
}

void VerifyHandler::newFile(const char* fname)
{
    minLSN = LogArchiver::ArchiveDirectory::parseLSN(fname, false);
    maxLSN = LogArchiver::ArchiveDirectory::parseLSN(fname, true);
    lastLSN = lsn_t::null;
    lastPID = lpid_t::null;
}

void VerifyHandler::invoke(logrec_t& r)
{
    lsn_t lsn = r.lsn_ck();
    lpid_t pid = r.construct_pid();
    assert(r.valid_header());
    assert(pid.page >= lastPID.page);
    if (pid.page == lastPID.page) {
        assert(lsn > lastLSN);
    }
    assert(merge || lsn >= minLSN);
    assert(merge || lsn <= maxLSN);

    lastLSN = lsn;
    lastPID = pid;

    count++;
}

void VerifyHandler::finalize()
{
    cout << "Log verification complete!" << endl;
    cout << "scanned_logrecs " << count << flushl;
}
