/* -*- mode:C++; c-basic-offset:4 -*-
     Shore-kits -- Benchmark implementations for Shore-MT

                       Copyright (c) 2007-2009
      Data Intensive Applications and Systems Labaratory (DIAS)
               Ecole Polytechnique Federale de Lausanne

                         All Rights Reserved.

   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/

/** @file:   shore_table.h
 *
 *  @brief:  Base class for tables stored in Shore
 *
 *  @note:   table_desc_t - table abstraction
 *
 *  @author: Ippokratis Pandis, January 2008
 *  @author: Caetano Sauer, April 2015
 *
 */


/* shore_table.h contains the base class (table_desc_t) for tables stored in
 * Shore. Each table consists of several parts:
 *
 * 1. An array of field_desc, which contains the decription of the
 *    fields.  The number of fields is set by the constructor. The schema
 *    of the table is not written to the disk.
 *
 * 2. The primary index of the table.
 *
 * 3. Secondary indices on the table.  All the secondary indices created
 *    on the table are stored as a linked list.
 *
 *
 * FUNCTIONALITY
 *
 * There are methods in (table_desc_t) for creating, the table
 * and indexes.
 *
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * @note  Modifications to the schema need rebuilding the whole
 *        database.
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 *
 * USAGE:
 *
 * To create a new table, create a class for the table by inheriting
 * publicly from class tuple_desc_t to take advantage of all the
 * built-in tools. The schema of the table should be set at the
 * constructor of the table.  (See shore_tpcc_schema.h for examples.)
 *
 *
 * NOTE:
 *
 * Due to limitation of Shore implementation, only the last field
 * in indexes can be variable length.
 *
 *
 * BUGS:
 *
 * If a new index is created on an existing table, explicit call to
 * load the index is needed.
 *
 * Timestamp field is not fully implemented: no set function.
 *
 *
 * EXTENSIONS:
 *
 * The mapping between SQL types and C++ types are defined in
 * (field_desc_t).  Modify the class to support more SQL types or
 * change the mapping.  The NUMERIC type is currently stored as string;
 * no further understanding is provided yet.
 *
 */

#ifndef __TABLE_DESC_H
#define __TABLE_DESC_H


#include "sm_vas.h"
#include "mcs_lock.h"

//#include "shore_msg.h"
#include "util/guard.h"

#include "file_desc.h"
#include "field.h"
#include "index_desc.h"
#include "row.h"

#include "util/zero_proxy.h"



/* ---------------------------------------------------------------
 *
 * @class: table_desc_t
 *
 * @brief: Description of a Shore table. Gives access to the fields,
 *         and indexes of the table.
 *
 * --------------------------------------------------------------- */

class table_desc_t
{
protected:

    pthread_mutex_t   _fschema_mutex;        // file schema mutex
    char              _name[MAX_FNAME_LEN];  // file name
    unsigned            _field_count;          // # of fields
    uint32_t           _pd;                   // info about the physical design

    /* ------------------- */
    /* --- table schema -- */
    /* ------------------- */

    ss_m*           _db;                 // the SM

    field_desc_t*   _desc;               // schema - set of field descriptors

    // primary index for index-organized table (replaces Heap of Shore-MT)
    index_desc_t*   _primary_idx;

    // secondary indexes
    std::vector<index_desc_t*>   _indexes;

    unsigned _maxsize;            // max tuple size for this table, shortcut

    vid_t _vid;

public:

    /* ------------------- */
    /* --- Constructor --- */
    /* ------------------- */

    table_desc_t(const char* name, int fieldcnt, uint32_t pd,
            vid_t vid = vid_t(1));
    virtual ~table_desc_t();


    /* ----------------------------------------- */
    /* --- create physical table and indexes --- */
    /* ----------------------------------------- */

    w_rc_t create_physical_table(ss_m* db);

    w_rc_t create_physical_index(ss_m* db, index_desc_t* index);


    /* ----------------------------------------------------- */
    /* --- create the logical description of the indexes --- */
    /* ----------------------------------------------------- */

    // create an index on the table
    bool   create_index_desc(const char* name,
                             const unsigned* fields,
                             const unsigned num,
                             const bool unique=true,
                             const bool primary=false,
                             const uint32_t& pd=PD_NORMAL);

    bool   create_primary_idx_desc(const char* name,
                                   const unsigned* fields,
                                   const unsigned num,
                                   const uint32_t& pd=PD_NORMAL);



    /* ------------------------ */
    /* --- index facilities --- */
    /* ------------------------ */

    // index by name
    index_desc_t* find_index(const char* index_name)
    {
        for (size_t i = 0; i < _indexes.size(); i++) {
            if (_indexes[i]->matches_name(index_name)) {
                return _indexes[i];
            }
        }
        return NULL;
    }

    std::vector<index_desc_t*>& get_indexes()
    {
        return _indexes;
    }

    // # of indexes
    int index_count() { return _indexes.size(); }

    index_desc_t* primary_idx() { return (_primary_idx); }
    stid_t get_primary_stid();

    /* sets primary index, the index itself should be already set to
     * primary and unique */
    void set_primary(index_desc_t* idx) {
        assert (idx->is_primary() && idx->is_unique());
        _primary_idx = idx;
    }

    char* index_keydesc(index_desc_t* idx);
    int   index_maxkeysize(index_desc_t* index) const; /* max index key size */

    /* ---------------------------------------------------------------- */
    /* --- for the conversion between disk format and memory format --- */
    /* ---------------------------------------------------------------- */

    unsigned maxsize(); /* maximum requirement for disk format */

    inline field_desc_t* desc(const unsigned descidx) {
        assert (descidx<_field_count);
        assert (_desc);
        return (&(_desc[descidx]));
    }

    const char*   name() const { return _name; }
    unsigned        field_count() const { return _field_count; }
    uint32_t       get_pd() const { return _pd; }

    /* ---------- */
    /* --- db --- */
    /* ---------- */
    void set_db(ss_m* db) { _db = db; }
    ss_m* db() { return (_db); }

    /* ----------------- */
    /* --- debugging --- */
    /* ----------------- */

    void print_desc(ostream & os = cout);  /* print the schema */

protected:
    int find_field_by_name(const char* field_name) const;

    srwlock_t _mutex;

}; // EOF: table_desc_t


typedef std::list<table_desc_t*> table_list_t;

#endif /* __TABLE_DESC_H */
