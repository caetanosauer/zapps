#define private public
#include "logcat.h"
#include "logarchiver.h"
#undef private

class PrintHandler : public Handler {
    virtual void invoke(logrec_t& r)
    {
        std::cout << r << endl;
    }

    virtual void finalize() {};
};

void LogCat::setupOptions()
{
    LogScannerCommand::setupOptions();
    options.add_options()
        ("file,f", po::value<string>(&filename)->default_value(""),
            "Scan only a specific file inside the given directory")
        ("archive,a", po::value<bool>(&isArchive)->default_value(false)
         ->implicit_value(true),
            "Scan log archive files isntead of normal recovery log")
    ;
}

void LogCat::run()
{
    PrintHandler* h = new PrintHandler();
    BaseScanner* s = isArchive ? getLogArchiveScanner() : getScanner();

    if (!filename.empty()) {
        s->setRestrictFile(logdir + "/" + filename);

        //DEBUG
    LogArchiver::ArchiveDirectory* directory = new
        // CS TODO -- fix block size bug (Issue #9)
        LogArchiver::ArchiveDirectory(logdir, 1024 * 1024);

        LogArchiver::ArchiveScanner::RunScanner* rs =
            new LogArchiver::ArchiveScanner::RunScanner(
                    lsn_t(1,322914768),
                    lsn_t(1,323974984),
                    lpid_t(1,2458), // first PID
                    lpid_t(1,2476), // last PID
                    25165824,            // file offset
                    directory
            );

        LogArchiver::ArchiveScanner::MergeHeapEntry r(rs);
        std::cout << r.active << std::endl;
    }

    s->type_handlers.resize(logrec_t::t_max_logrec);
    s->any_handlers.push_back(h);
    s->fork();
    s->join();

    delete s;
    delete h;

}


