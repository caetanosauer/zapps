#include "command.h"

//#include "commands/logreplay.h"
//#include "commands/verifylog.h"
//#include "commands/dbstats.h"
//#include "commands/agglog.h"
//#include "commands/mergerestore.h"
//#include "commands/cat.h"
//#include "commands/skew.h"
//#include "commands/dirtypagestats.h"
//#include "commands/trace.h"

#include "kits_cmd.h"
#include "genarchive.h"
#include "agglog.h"
#include "logcat.h"
#include "verifylog.h"
#include "experiments/restore_cmd.h"

/*
 * Adapted from
 * http://stackoverflow.com/questions/582331/is-there-a-way-to-instantiate-objects-from-a-string-holding-their-class-name
 */
Command::ConstructorMap Command::constructorMap;

template<typename T> Command* createCommand()
{
    return new T;
}

#define REGISTER_COMMAND(str, cmd) \
{ \
    Command::constructorMap[str] = &createCommand<cmd>; \
}

void Command::init()
{
    /*
     * COMMANDS MUST BE REGISTERED HERE AND ONLY HERE
     */
    REGISTER_COMMAND("logcat", LogCat);
    //REGISTER_COMMAND("skew", skew);
    //REGISTER_COMMAND("trace", trace);
    //REGISTER_COMMAND("dirtypagestats", dirtypagestats);
    //REGISTER_COMMAND("logreplay", LogReplay);
    REGISTER_COMMAND("genarchive", GenArchive);
    REGISTER_COMMAND("verifylog", VerifyLog);
    //REGISTER_COMMAND("dbstats", DBStats);
    REGISTER_COMMAND("agglog", AggLog);
    //REGISTER_COMMAND("mrestore", MergeRestore);
    REGISTER_COMMAND("kits", KitsCommand);
    REGISTER_COMMAND("restore", RestoreCmd);
}


void Command::showCommands()
{
    cerr << "Usage: zapps <command> [options] "
        << endl << "Commands:" << endl;
    ConstructorMap::iterator it;
    for (it = constructorMap.begin(); it != constructorMap.end(); it++) {
        Command* cmd = (it->second)();
        cmd->setupOptions();
        cerr << it->first << endl << cmd->options << endl << endl;
    }
}

Command* Command::parse(int argc, char ** argv)
{
    if (argc >= 2) {
        string cmdStr = argv[1];
        std::transform(cmdStr.begin(), cmdStr.end(), cmdStr.begin(), ::tolower);
        if (constructorMap.find(cmdStr) != constructorMap.end()) {
            Command* cmd = constructorMap[cmdStr]();
            cmd->setCommandString(cmdStr);
            cmd->setupOptions();
            po::variables_map vm;
            po::store(po::parse_command_line(argc, argv, cmd->getOptions()), vm);
            po::notify(vm);
            cmd->setOptionValues(vm);

            return cmd;
        }
    }

    showCommands();
    return NULL;
}

size_t LogScannerCommand::BLOCK_SIZE = 1024 * 1024;

BaseScanner* LogScannerCommand::getScanner(
        bitset<logrec_t::t_max_logrec>* filter)
{
    return new BlockScanner(logdir.c_str(), BLOCK_SIZE, filter);
}

BaseScanner* LogScannerCommand::getLogArchiveScanner()
{
    return new LogArchiveScanner(logdir);
}

BaseScanner* LogScannerCommand::getMergeScanner()
{
    return new MergeScanner();
}

void LogScannerCommand::setupOptions()
{
    options.add_options()
        ("logdir,l", po::value<string>(&logdir)->required(),
            "Directory containing log to be scanned")
        ("n", po::value<size_t>(&limit)->default_value(0),
            "Number of log records to scan")
    ;
}

