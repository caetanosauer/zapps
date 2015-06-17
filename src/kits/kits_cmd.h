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
    string opt_conffile;
    int opt_num_trxs;
    int opt_num_threads;
    int opt_select_trx;
    int opt_queried_sf;
    bool opt_spread;

    template<class Client, class Environment> void runBenchmark();
};

#endif
