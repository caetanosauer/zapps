#ifndef LOGCAT_H
#define LOGCAT_H

#include "command.h"

class LogCat : public LogScannerCommand {
public:
    void usage();
    void run();
    void setupOptions();

private:
    bool isArchive;
    string filename;
    size_t number;
};

#endif
