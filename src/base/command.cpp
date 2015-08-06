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
		string pathToFile = "shore.conf";
		string cmdStr = argv[1];
		std::transform(cmdStr.begin(), cmdStr.end(), cmdStr.begin(), ::tolower);
		if (constructorMap.find(cmdStr) != constructorMap.end()) {
			Command* cmd = constructorMap[cmdStr]();
			cmd->options.add_options()
        		("help,h", "Displays help information regarding a specific command");
			cmd->setCommandString(cmdStr);
			cmd->setupOptions();
			po::variables_map vm;
			po::store(po::parse_command_line(argc,argv,cmd->getOptions()), vm);
			if(vm.count("config-file")){
				pathToFile = vm["config-file"].as<string>();
				std::ifstream file;
				file.open(pathToFile.c_str());
				po::store(po::parse_config_file(file,cmd->getOptions(),true), vm);
			}
			if(vm.count("help")){
				cmd->helpOption();
				return NULL;
			}
			po::notify(vm);
			cmd->setOptionValues(vm);
			return cmd;
		}
	}

	showCommands();
	return NULL;
}
void Command::setupSMOptions(){
	boost::program_options::options_description smoptions("Storage Manager Options");
	smoptions.add_options()
	 ("db-config-design", po::value<string>()->default_value("normal"),
	   "")
	("physical-hacks-enable", po::value<int>()->default_value(0),
		"")
	("db-worker-sli", po::value<bool>()->default_value(0),
		"")
	("db-loaders", po::value<int>()->default_value(10),
		"")
	("db-worker-queueloops", po::value<int>()->default_value(10),
				"")
	("db-cl-batchsz", po::value<int>()->default_value(10),
			    "")
	("db-cl-thinktime", po::value<int>()->default_value(0),
			"")
	("records-to-access", po::value<uint>()->default_value(0),
		"")
	("activation_delay", po::value<uint>()->default_value(0),
			"")
	("db-workers", po::value<uint>()->default_value(1),
		"")
	("dir-trace", po::value<string>()->default_value("RAT"),
		"")
	("config-file", po::value<string>(),
		"")
	/** System related options **/
	("sys-maxcpucount", po::value<uint>()->default_value(0),
		"")
	("sys-activecpucount", po::value<uint>()->default_value(0),
		"")
	/**SM Options**/
	("sm_fakeiodelay-enable", po::value<int>()->default_value(0),
		"")
	("sm_fakeiodelay", po::value<uint>()->default_value(0),
			"")
	("sm_errlog", po::value<string>()->default_value("shoremt.err.log"),
			"")
	("sm_pagecleaners", po::value<uint>()->default_value(16),
						"")
	("sm_chkpt_flush_interval", po::value<uint>()->default_value(-1),
		"")
	("sm_backgroundflush", po::value<uint>()->default_value(1),
		"")
	("sm_log_page_flushers", po::value<uint>()->default_value(1),
		"")
	("sm_preventive_chkpt", po::value<uint>()->default_value(1),
		"")
	("sm_logbuf_seg_count", po::value<int>(),
		"")
	("sm_logbuf_flush_trigger", po::value<int>(),
		"")
	("sm_logbuf_block_size", po::value<int>(),
		"")
	("sm_logbuf_part_size", po::value<int>(),
		"")
	("sm_carray_slots", po::value<int>(),
		"")
	("sm_restart", po::value<int>(),
		"")
	("sm_restore_segsize", po::value<int>(),
		"")
	("sm_rawlock_gc_interval_ms", po::value<int>(),
		"")
	("sm_rawlock_lockpool_segsize", po::value<int>(),
		"")
	("sm_rawlock_xctpool_segsize", po::value<int>(),
		"")
	("sm_rawlock_gc_generation_count", po::value<int>(),
		"")
	("sm_rawlock_gc_init_generation_count", po::value<int>(),
		"")
	("sm_rawlock_lockpool_initseg", po::value<int>(),
		"")
	("sm_rawlock_xctpool_segsize", po::value<int>(),
		"")
	("sm_rawlock_gc_free_segment_count", po::value<int>(),
		"")
	("sm_rawlock_gc_max_segment_count", po::value<int>(),
		"")
	("sm_locktablesize", po::value<int>(),
		"")
	("sm_rawlock_xctpool_initseg", po::value<int>(),
		"")
	("sm_num_page_writers", po::value<int>(),
		"")
	("sm_cleaner_interval_millisec_min", po::value<int>(),
		"")
	("sm_cleaner_interval_millisec_max", po::value<int>(),
		"")
	("sm_cleaner_write_buffer_pages", po::value<int>(),
		"")
	("sm_archiver_workspace_size", po::value<int>(),
		"")
	("sm_archiver_block_size", po::value<int>(),
		"")
	("sm_merge_factor", po::value<int>(),
		"")
	("sm_archiving_blocksize", po::value<int>(),
		"")
	("sm_reformat_log", po::value<bool>(),
		"")
	("sm_logging", po::value<bool>(),
		"")
	("sm_archiving", po::value<bool>(),
		"")
	("sm_async_merging", po::value<bool>(),
		"")
	("sm_statistics", po::value<bool>(),
		"")
	("sm_ticker_enable", po::value<bool>(),
		"")
	("sm_ticker_msec", po::value<bool>(),
		"")
	("sm_prefetch", po::value<bool>(),
		"")
	("sm_restore_instant", po::value<bool>(),
		"")
	("sm_restore_reuse_buffer", po::value<bool>(),
		"")
	("sm_bufferpool_swizzle", po::value<bool>(),
		"")
	("sm_archiver_eager", po::value<bool>(),
		"")
	("sm_archiver_read_whole_blocks", po::value<bool>(),
		"")
	("sm_archiver_slow_log_grace_period", po::value<int>(),
		"")
	("sm_errlog_level", po::value<string>(),
		"")
	("sm_log_impl", po::value<string>(),
		"")
	("sm_backup_dir", po::value<string>(),
		"")
	("sm_bufferpool_replacement_policy", po::value<string>(),
		"")
	("sm_archdir", po::value<string>(),
		"");
	options.add(smoptions);
}

void Command::helpOption(){
	cerr << "Usage: zapps Command:" << commandString << " [options] "
				<< endl << options << endl;
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
    return new MergeScanner(logdir);
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

