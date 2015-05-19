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

/** @file shore_table.cpp
 *
 *  @brief Implementation of shore_table class
 *
 *  @author: Ippokratis Pandis, January 2008
 *  @author: Caetano Sauer, April 2015
 *
 */

#include "table_desc.h"

#include "w_key.h"

table_desc_t::table_desc_t(const char* name, int fieldcnt, uint32_t pd)
    : file_desc_t(name, fieldcnt, pd), _db(NULL),
      _indexes(NULL), _primary_idx(NULL),
      _maxsize(0)
{
    // Create placeholders for the field descriptors
    _desc = new field_desc_t[fieldcnt];
}


table_desc_t::~table_desc_t()
{
    if (_desc) {
        delete [] _desc;
        _desc = NULL;
    }

    if (_indexes) {
        delete _indexes;
        _indexes = NULL;
    }
}


/* ----------------------------------------- */
/* --- create physical table and indexes --- */
/* ----------------------------------------- */


/*********************************************************************
 *
 *  @fn:    create_physical_table
 *
 *  @brief: Creates the physical table and calls for the (physical) creation of
 *          all the corresponding indexes
 *
 *********************************************************************/

w_rc_t table_desc_t::create_physical_table(ss_m* db)
{
    assert (db);
    _db = db;

    if (!is_vid_valid() || !is_root_valid()) {
	W_DO(find_root_iid(db));
    }


    // Create the table
    index_desc_t* index = _indexes;

#warning TODO CS -- implement this

    TRACE( TRACE_STATISTICS, "%s %d\n", name(), fid().store);

    // Add table entry to the metadata tree
    file_info_t file;
    file.set_ftype(FT_TABLE);
    file.set_fid(_fid);
    w_keystr_t kstr;
    kstr.construct_regularkey(name(), strlen(name()));
    W_DO(ss_m::create_assoc(root_iid(),
			    kstr,
			    vec_t(&file, sizeof(file_info_t))));


    // Create all the indexes of the table
    while (index) {

        // Create physically this index
        W_DO(create_physical_index(db,index));

        // Move to the next index of the table
	index = index->next();
    }

    return (RCOK);
}



/*********************************************************************
 *
 *  @fn:    create_physical_index
 *
 *  @brief: Creates the physical index
 *
 *********************************************************************/

w_rc_t table_desc_t::create_physical_index(ss_m* db, index_desc_t* index)
{
    // Store info
    file_info_t file;

    // Create all the indexes of the table
    stid_t iid = stid_t::null;

    // if it is the primary, update file flag
    if (index->is_primary()) {
        file.set_ftype(FT_PRIMARY_IDX);
    }
    else {
        file.set_ftype(FT_IDX);
    }

    if (index->is_rmapholder()) {
        file.set_ftype(FT_NONE);
    }


    // create one index or multiple, if the index is partitioned
            W_DO(db->create_index(_vid, iid));
        index->set_fid(iid);

        // Add index entry to the metadata tree
        file.set_fid(iid);
        w_keystr_t kstr;
        kstr.construct_regularkey(index->name(), strlen(index->name()));
        W_DO(db->create_assoc(root_iid(),
                              kstr,
                              vec_t(&file, sizeof(file_info_t))));

    // Print info
    TRACE( TRACE_STATISTICS, "%s %d (%s) (%s) (%s) (%s) (%s)\n",
           index->name(), iid.store,
           (index->is_latchless() ? "no latch" : "latch"),
           (index->is_relaxed() ? "relaxed" : "no relaxed"),
           (index->is_unique() ? "unique" : "no unique"));

    return (RCOK);
}

/******************************************************************
 *
 *  @fn:    create_index_desc
 *
 *  @brief: Create the description of a regular or primary index on the table
 *
 *  @note:  This only creates the index decription for the index in memory.
 *
 ******************************************************************/

#warning Cannot update fields included at indexes - delete and insert again

#warning Only the last field of an index can be of variable length

bool table_desc_t::create_index_desc(const char* name,
                                     const unsigned* fields,
                                     const unsigned num,
                                     const bool unique,
                                     const bool primary,
                                     const uint32_t& pd)
{
    index_desc_t* p_index = new index_desc_t(name, num, fields,
                                             unique, primary, pd);

    // check the validity of the index
    for (unsigned i=0; i<num; i++)  {
        assert(fields[i] < _field_count);

        // only the last field in the index can be variable lengthed
#warning IP: I am not sure if still only the last field in the index can be variable lengthed

        if (_desc[fields[i]].is_variable_length() && i != num-1) {
            assert(false);
        }
    }

    // link it to the list
    if (_indexes == NULL) _indexes = p_index;
    else _indexes->insert(p_index);

    // add as primary
    if (p_index->is_unique() && p_index->is_primary())
        _primary_idx = p_index;

    return true;
}


bool table_desc_t::create_primary_idx_desc(const char* name,
                                           const unsigned* fields,
                                           const unsigned num,
                                           const uint32_t& pd)
{
    index_desc_t* p_index = new index_desc_t(name, num, fields,
                                             true, true, pd);

    // check the validity of the index
    for (unsigned i=0; i<num; i++) {
        assert(fields[i] < _field_count);

        // only the last field in the index can be variable lengthed
        if (_desc[fields[i]].is_variable_length() && i != num-1) {
            assert(false);
        }
    }

    // link it to the list of indexes
    if (_indexes == NULL) _indexes = p_index;
    else _indexes->insert(p_index);

    // make it the primary index
    _primary_idx = p_index;

    return (true);
}


// Returns the stid of the primary index. If no primary index exists it
// returns the stid of the table
stid_t table_desc_t::get_primary_stid()
{
    stid_t stid = ( _primary_idx ? _primary_idx->fid() : fid() );
    return (stid);
}


/* ----------------- */
/* --- debugging --- */
/* ----------------- */


// For debug use only: print the description for all the field
void table_desc_t::print_desc(ostream& os)
{
    os << "Schema for table " << _name << endl;
    os << "Numer of fields: " << _field_count << endl;
    for (unsigned i=0; i<_field_count; i++) {
	_desc[i].print_desc(os);
    }
}
