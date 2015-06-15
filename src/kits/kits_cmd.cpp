#include "kits_cmd.h"

#include <stdexcept>

#include "shore_env.h"
#include "tpcb/tpcb_env.h"

void KitsCommand::setupOptions()
{
    options.add_options()
        ("benchmark,b", po::value<string>(&opt_benchmark)->required(),
            "Benchmark to execute. Possible values: tpcb, tpcc")
    ;
}

void KitsCommand::run()
{
    // just TPC-B for now
    if (opt_benchmark == "tpcb") {
        shoreEnv = new tpcb::ShoreTPCBEnv();
    }
    else {
        throw runtime_error("Unknown benchmark string");
    }
}
