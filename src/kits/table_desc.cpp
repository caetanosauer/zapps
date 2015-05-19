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
      _maxsize(0),
      _sMinKey(NULL),_sMinKeyLen(0),
      _sMaxKey(NULL),_sMaxKeyLen(0)
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

    if (_sMinKey!=NULL) {
        free(_sMinKey);
        _sMinKey=NULL;
    }

    if (_sMaxKey!=NULL) {
        free(_sMaxKey);
        _sMaxKey=NULL;
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
    file.set_ftype(FT_HEAP);
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
    ss_m::ndx_t smidx_type = ss_m::t_uni_btree;
    ss_m::concurrency_t smidx_cc = ss_m::t_cc_im;

    // Update the type of index to create


    if (index->is_mr()) {
        W_FATAL_MSG(fcINTERNAL, << "Zero does not support multi-root B-trees");
    }
    else {
        // Regular BTree
        smidx_type = index->is_unique() ? ss_m::t_uni_btree : ss_m::t_btree;
    }


    // what kind of CC will be used
    smidx_cc = index->is_relaxed() ? ss_m::t_cc_none : ss_m::t_cc_keyrange;


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
    if(index->is_partitioned()) {
        for(int i=0; i < index->get_partition_count(); i++) {
            if (!index->is_mr()) {
                W_DO(db->create_index(_vid, smidx_type, ss_m::t_regular,
                                      index_keydesc(index), smidx_cc, iid));
            }
            else {
                W_FATAL_MSG(fcINTERNAL,
                        << "Zero does not support multi-root B-trees");
            }
            index->set_fid(i, iid);

            // add index entry to the metadata tree
            file.set_fid(iid);
            char tmp[100];
            sprintf(tmp, "%s_%d", index->name(), i);
            w_keystr_t kstr;
            kstr.construct_regularkey(tmp, strlen(tmp));
            W_DO(db->create_assoc(root_iid(),
                                  kstr,
                                  vec_t(&file, sizeof(file_info_t))));
        }
    }
    else {

        if (!index->is_mr()) {
            W_DO(db->create_index(_vid, smidx_type, ss_m::t_regular,
                                  index_keydesc(index), smidx_cc, iid));
        }
        else {
            W_FATAL_MSG(fcINTERNAL,
                    << "Zero does not support multi-root B-trees");
        }
        index->set_fid(0, iid);

        // Add index entry to the metadata tree
        file.set_fid(iid);
        w_keystr_t kstr;
        kstr.construct_regularkey(index->name(), strlen(index->name()));
        W_DO(db->create_assoc(root_iid(),
                              kstr,
                              vec_t(&file, sizeof(file_info_t))));
    }

    // Print info
    TRACE( TRACE_STATISTICS, "%s %d (%s) (%s) (%s) (%s) (%s)\n",
           index->name(), iid.store,
           (index->is_mr() ? "mrbt" : "normal"),
           (index->is_latchless() ? "no latch" : "latch"),
           (index->is_relaxed() ? "relaxed" : "no relaxed"),
           (index->is_partitioned() ? "part" : "no part"),
           (index->is_unique() ? "unique" : "no unique"));

    return (RCOK);
}



/******************************************************************
 *
 *  @fn:    create_physical_empty_primary_idx_desc
 *
 *  @brief: Creates the description of an empty MRBT index with the
 *          partitioning information, and physically adds it to the
 *          sm
 *
 *  @note:  It is used for tables without any indexes or for tables
 *          whose ex-primary index is manually partitioned (hacked)
 *
 ******************************************************************/

w_rc_t table_desc_t::create_physical_empty_primary_idx()
{
    // The table should have already been physically created
    assert (_db);

    string idxname = string(this->name()) + string("MRHolder");
    unsigned key[1]={0};
    index_desc_t* p_index = new index_desc_t(idxname.c_str(),1,0,key,
                                             false,false,PD_MRBT_NORMAL,
                                             true);
    assert(p_index);

    // make it the primary index
    _primary_idx = p_index;

    W_DO(create_physical_index(_db,p_index));

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
                                     int partitions,
                                     const unsigned* fields,
                                     const unsigned num,
                                     const bool unique,
                                     const bool primary,
                                     const uint32_t& pd)
{
    index_desc_t* p_index = new index_desc_t(name, num, partitions, fields,
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
                                           int partitions,
                                           const unsigned* fields,
                                           const unsigned num,
                                           const uint32_t& pd)
{
    index_desc_t* p_index = new index_desc_t(name, num, partitions, fields,
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
    stid_t stid = ( _primary_idx ? _primary_idx->fid(0) : fid() );
    return (stid);
}




/* ---------------------------------------------------- */
/* --- partitioning information, used with MRBTrees --- */
/* ---------------------------------------------------- */

w_rc_t table_desc_t::set_partitioning(const char* sMinKey,
                                      unsigned len1,
                                      const char* sMaxKey,
                                      unsigned len2,
                                      unsigned numParts)
{
    // Allocate for the two boundaries
    if (_sMinKeyLen < len1) {
        if (_sMinKey) { free (_sMinKey); }
        _sMinKey = (char*)malloc(len1+1);
    }
    memset(_sMinKey,0,len1+1);
    memcpy(_sMinKey,sMinKey,len1);
    _sMinKeyLen = len1;

    if (_sMaxKeyLen < len2) {
        if (_sMaxKey) { free (_sMaxKey); }
        _sMaxKey = (char*)malloc(len2+1);
    }
    memset(_sMaxKey,0,len2+1);
    memcpy(_sMaxKey,sMaxKey,len2);
    _sMaxKeyLen = len2;

    _numParts = numParts;
    return (RCOK);
}


// Accessing information about the main partitioning
unsigned table_desc_t::pcnt() const
{
    return (_numParts);
}

char* table_desc_t::getMinKey() const
{
    return (_sMinKey);
}

unsigned table_desc_t::getMinKeyLen() const
{
    return (_sMinKeyLen);
}

char* table_desc_t::getMaxKey() const
{
    return (_sMaxKey);
}

unsigned table_desc_t::getMaxKeyLen() const
{
    return (_sMaxKeyLen);
}


/******************************************************************
 *
 *  @fn:    get_main_rangemap
 *
 *  @brief: Returns a pointer to the RangeMap of the primary index.
 *          If there is no primary index, it creates an empty index
 *          with a RangeMap based on the partitioning info
 *
 ******************************************************************/


w_rc_t table_desc_t::get_main_rangemap(key_ranges_map*& rangemap)
{
    if (!_primary_idx || _primary_idx->is_partitioned()) {
        // Create an empty "primary" index which is not manually partitioned
        create_physical_empty_primary_idx();
    }
    assert (_primary_idx);

    // Cannot have a manually (hacked) partitioned index as primary
    // To do that, use an empty index as primary
    assert (!_primary_idx->is_partitioned());

    W_DO(ss_m::get_range_map(_primary_idx->fid(0),rangemap));
    return (RCOK);
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
