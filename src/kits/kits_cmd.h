#ifndef KITS_CMD_H
#define KITS_CMD_H

#include "command.h"

class ShoreEnv;
class sm_options;

class KitsCommand : public Command
{
public:
    virtual void setupOptions();
    virtual void run();
protected:
    ShoreEnv* shoreEnv;

    string logdir;
    string archdir;
    string opt_dbfile;

    bool opt_load;
    string opt_benchmark;
    string opt_conffile;
    int opt_bufsize;
    int opt_num_trxs;
    int opt_num_threads;
    int opt_select_trx;
    int opt_queried_sf;

    bool opt_spread;

    // overridden in sub-commands to set their own options
    virtual void loadOptions(sm_options& opt);

    template<class Client, class Environment> void runBenchmarkSpec();
    void runBenchmark();

    template <class Environment> void initShoreEnv();
    void init();
    void finish();

    // Filesystem functions
    void mkdirs(string);
    void ensureEmptyPath(string);
};

#endif
