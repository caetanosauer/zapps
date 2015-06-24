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

    void archiveLog();
};

#endif
