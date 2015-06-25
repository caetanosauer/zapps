#ifndef RESTORE_CMD_H
#define RESTORE_CMD_H

#include "kits_cmd.h"

class RestoreCmd : public KitsCommand
{
public:
    virtual void setupOptions();
    virtual void run();

protected:
    string opt_backup;
    unsigned opt_segmentSize;
    bool opt_singlePass;
    bool opt_instant;
    bool opt_evict;
    bool opt_crash;
    int opt_crashDelay;
    float opt_postRestoreWorkFactor;
    bool opt_concurrentArchiving;

    void archiveLog();
};

#endif
