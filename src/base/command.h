#ifndef COMMAND_H
#define COMMAND_H

#include "zapps-config.h"

#define SM_SOURCE
#include <sm_int_4.h>

#include "basethread.h"
#include "handler.h"
#include "scanner.h"
#include <boost/program_options.hpp>

namespace po = boost::program_options;

class Command : public basethread_t {
public:
    virtual void run() = 0;
    virtual void setupOptions() = 0;

    static Command* parse(int argc, char** argv);
    static void init();

    po::options_description& getOptions() { return options; }
    void setCommandString(string s) { commandString = s; }
    void setOptionValues(po::variables_map& vm) { optionValues = vm; }

protected:
    po::options_description options;
    po::variables_map optionValues;

    string commandString;

private:
    typedef map<string, Command*(*)()> ConstructorMap;
    static ConstructorMap constructorMap;

    static void showCommands();
};

class LogScannerCommand : public Command {
public:
    static size_t BLOCK_SIZE;

    virtual void setupOptions();
protected:
    BaseScanner* getScanner(bool archive = false);
    BaseScanner* getMergeScanner();

    string logdir; // TODO make private once scan_thread_t is gone
private:
    BaseScanner* scanner;
};

#endif
