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
        ("merge,m", po::value<bool>(&merge)->default_value(false)
         ->implicit_value(true),
            "Merge archiver input so that global sort order is produced")
    ;
}

void LogCat::run()
{
    PrintHandler* h = new PrintHandler();
    BaseScanner* s = isArchive ?
        (merge ? getMergeScanner() : getLogArchiveScanner())
        : getScanner();

    if (!filename.empty()) {
        s->setRestrictFile(logdir + "/" + filename);
    }

    s->type_handlers.resize(logrec_t::t_max_logrec);
    s->any_handlers.push_back(h);
    s->fork();
    s->join();

    delete s;
    delete h;

}


