#include "agglog.h"

void AggLog::setupOptions()
{
    LogScannerCommand::setupOptions();
    options.add_options()
        ("type,t", po::value<vector<string> >(&typeStrings)->multitoken(),
            "Log record types to be considered by the aggregator")
        ("interval,i", po::value<int>(&interval)->default_value(1),
            "Size of the aggregation groups in number of ticks (default 1)")
    ;
}

void AggLog::run()
{
    bitset<logrec_t::t_max_logrec> filter;
    filter.reset();

    // set filter bit for all valid logrec types found in the arguments given
    for (int i = 0; i < logrec_t::t_max_logrec; i++) {
        auto it = find(typeStrings.begin(), typeStrings.end(),
                string(logrec_t::get_type_str((logrec_t::kind_t) i)));

        if (it != typeStrings.end()) {
            filter.set(i);
        }
    }

    if (filter.none()) {
        throw runtime_error("AggLog requires at least one valid logrec type");
    }

    AggregateHandler h(filter, interval);

    // filter must not ignore tick log records
    filter.set(logrec_t::t_tick_sec);
    filter.set(logrec_t::t_tick_msec);
    BaseScanner* s = getScanner(&filter);
    s->any_handlers.push_back(&h);
    s->fork();
    s->join();
    delete s;
}

AggregateHandler::AggregateHandler(bitset<logrec_t::t_max_logrec> filter,
        int interval)
    : filter(filter), interval(interval)
{
    assert(interval > 0);
    counts.reserve(logrec_t::t_max_logrec);
    for (size_t i = 0; i < logrec_t::t_max_logrec; i++) {
        counts[i] = 0;
    }

    // print header line with type names
    cout << "#";
    for (int i = 0; i < logrec_t::t_max_logrec; i++) {
        if (filter[i]) {
            cout << " " << logrec_t::get_type_str((logrec_t::kind_t) i);
        }
    }
    cout << flushl;
}

void AggregateHandler::invoke(logrec_t& r)
{
    if (r.type() == logrec_t::t_tick_sec || r.type() == logrec_t::t_tick_msec) {
        currentTick++;
        if (currentTick == interval) {
            currentTick = 0;
            dumpCounts();
        }
    }
    else if (filter[r.type()]) {
        counts[r.type()]++;
    }
}

void AggregateHandler::dumpCounts()
{
    for (size_t i = 0; i < counts.capacity(); i++) {
        if (filter[i]) {
            cout << counts[i] << '\t';
            counts[i] = 0;
        }
    }
    cout << flushl;
}

void AggregateHandler::finalize()
{
    dumpCounts();
}
