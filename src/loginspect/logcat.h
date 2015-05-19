#ifndef LOGCAT_H
#define LOGCAT_H

#include "command.h"

class LogCat : public LogScannerCommand {
public:
    void usage();
    void run();
    void setupOptions();

private:
    size_t number;
};

#endif
