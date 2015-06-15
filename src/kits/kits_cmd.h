#ifndef KITS_CMD_H
#define KITS_CMD_H

#include "command.h"

class ShoreEnv;

class KitsCommand : public Command
{
public:
    virtual void setupOptions();
    virtual void run();
private:
    ShoreEnv* shoreEnv;

    string opt_benchmark;
};

#endif
