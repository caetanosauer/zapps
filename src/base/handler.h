#ifndef HANDLER_H
#define HANDLER_H

#include <iostream>
#include <sstream>
#include <unordered_set>
#include <algorithm> 

class handler {
public:
    //invoke performs handler activity
    virtual void invoke(logrec_t &r) = 0;
    virtual void finalize() = 0;

    virtual void newFile(const char* /* fname */) {};
};

class PageHandler {
public:
    virtual void finalize() {};
    virtual void handle(const lpid_t& pid) = 0;
};

class StoreHandler {
public:
    virtual void finalize() {};
    virtual void handle(const stid_t&) = 0;
};

#endif
