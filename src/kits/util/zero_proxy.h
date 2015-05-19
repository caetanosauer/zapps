/*
 * (c) Copyright 2011-2013, Hewlett-Packard Development Company, LP
 */

#ifndef ZERO_PROXY_H
#define ZERO_PROXY_H

#include "sm_base.h"
#include "w_okvl_inl.h"

#ifndef XCT_DEPENDENT_H
#include <xct_dependent.h>
#endif /* XCT_DEPENDENT_H */

class bt_cursor_t;

typedef okvl_mode lock_mode_t;
okvl_mode SH = ALL_S_GAP_S;
okvl_mode EX = ALL_X_GAP_X;
okvl_mode NL = ALL_N_GAP_N;

/**\brief Iterator over an index.
 * \details
 * \ingroup SSMSCANI
 * To iterate over the {key,value} pairs in an index,
 * construct an instance of this class,
 * and use its next() method to advance the cursor and the curr() method
 * to copy out keys and values into server-space.
 * It is unwise to delete or insert associations while you have a scan open on
 * the index (in the same transaction).
 *
 * Example code:
 * \code
 * stid_t fid(1,7);
 * scan_index_i scan(fid,
                scan_index_i::ge, vec_t::neg_inf,
                scan_index_i::le, vec_t::pos_inf, false,
                ss_m::t_cc_kvl);
 * bool         eof(false);
 * do {
 *    w_rc_t rc = scan.next(eof);
 *    if(rc.is_error()) {
 *       // handle error
 *       ...
 *    }
 *    if(eof) break;
 *
 *   // get the key len and element len
 *   W_DO(scan.curr(NULL, klen, NULL, elen));
 *
 *   // Create vectors for the given lengths.
 *   vec_t key(keybuf, klen);
 *   vec_t elem(&info, elen);
 *
 *   // Get the key and element value
 *   W_DO(scan.curr(&key, klen, &elem, elen));
 *    ...
 * } while (1);
 * \endcode
 *
 */
class scan_index_i : public smlevel_top, public xct_dependent_t {
public:
    /**\brief Construct an iterator.
     * \details
     * @param[in] stid   ID of the B-Tree to be scanned.
     * @param[in] c1     Comparison type to be used in the scan for
     *                   lower bound comparison :
     *                   eq, gt, ge, lt, le
     * @param[in] bound1 Lower bound
     * @param[in] c2     Comparison type to be used in the scan for
     *                   upper bound comparison :
     *                   eq, gt, ge, lt, le
     * @param[in] bound2 Upper bound
     * @param[in] include_nulls   If true, we will consider null keys
     *        as satisfying the condition.
     * @param[in] cc   Ignored in Zero
     * @param[in] mode Ignored in Zero
     */
    NORET            scan_index_i(
        const stid_t&             stid,
        cmp_t                     c1,
        const cvec_t&             bound1,
        cmp_t                     c2,
        const cvec_t&             bound2,
        bool                      include_nulls = false,
        concurrency_t             cc = t_cc_keyrange, // mode used in Zero
        lock_mode_t               mode = SH,
        const bool                bIgnoreLatches = false
        );


    NORET            ~scan_index_i();

    /**brief Get the key and value where the cursor points.
     *
     * @param[out] key   Pointer to vector supplied by caller.  curr() writes into this vector.
     * A null pointer indicates that the caller is not interested in the key.
     * @param[out] klen   Pointer to sm_size_t variable.  Length of key is written here.
     * @param[out] el   Pointer to vector supplied by caller.  curr() writes into this vector.
     * A null pointer indicates that the caller is not interested in the value.
     * @param[out] elen   Pointer to sm_size_t variable.  Length of value is written here.
     */
    rc_t            curr(
        vec_t*           key,
        smsize_t&        klen,
        vec_t*           el,
        smsize_t&        elen)  {
            return _fetch(key, &klen, el, &elen, false);
        }

    /**brief Move to the next key-value pair in the index.
     *
     * @param[out] eof   True is returned if there are no more pairs in the index.
     * If false, curr() may be called.
     */
    rc_t             next(bool& eof)  {
        rc_t rc = _fetch(0, 0, 0, 0, true);
        eof = _eof;
        return rc;
        // In Kits, it was "return rc.reset()", but Zero does not have reset()
    }

    /// Free the resources used by this iterator. Called by desctructor if
    /// necessary.
    void             finish();

    /// If false, curr() may be called.
    bool             eof()    { return _eof; }

    /// If false, curr() may be called.
    tid_t            xid() const { return tid; }
    const rc_t &     error_code() const { return _error_occurred; }

private:
    stid_t               _stid;
    tid_t                tid;
    cmp_t                cond2;
    bool                 _eof;

    w_rc_t               _error_occurred;
    bt_cursor_t*         _btcursor;
    bool                 _finished;
    bool                 _skip_nulls;

    rc_t            _fetch(
        vec_t*                key,
        smsize_t*             klen,
        vec_t*                el,
        smsize_t*             elen,
        bool                  skip);

    void            xct_state_changed(
        xct_state_t            old_state,
        xct_state_t            new_state);

    // disabled
    NORET            scan_index_i(const scan_index_i&);
    scan_index_i&    operator=(const scan_index_i&);
};


// some of these includes need shore_scan_i
//#include "iter.h"
#include "row.h"
#include "table_desc.h"
//#include "table_man.h"


/**
 * Implementation of a table scan based on Zero's Foster B-tree indices.
 * Reuses the infrastructure of the generic class tuple_iter_t.
 * Shore has its own implementation in table_scan_iter_impl, which uses
 * the Shore-MT class scan_file_i to scan a heapfile.
 * We use the same class name to avoid changes in the benchmark code.
 * To avoid the name colision, Shore's version of the class is ignored
 * using the CFG_ZERO flag (see shore_iter.h)
 */
#if 0
template <class TableDesc>
class table_scan_iter_impl
    : public tuple_iter_t<TableDesc, scan_index_i, table_row_t>
{
public:
    typedef table_man_impl<TableDesc> table_manager;
    typedef tuple_iter_t<TableDesc, scan_index_i, table_row_t> table_iter;

private:
    table_manager* _pmanager;
    w_keystr_t neg_inf;
    w_keystr_t pos_inf;
    lpid_t fake_pid;
    shpid_t fake_shpid;
    slotid_t fake_slot;
    static const slotid_t max_slot;

public:

    table_scan_iter_impl(ss_m* db,
                         TableDesc* ptable,
                         table_manager* pmanager,
                         lock_mode_t alm)
        : table_iter(db, ptable, alm, true), _pmanager(pmanager)
    {
        assert (_pmanager);
        W_COERCE(open_scan(db));
        assert(neg_inf.construct_neginfkey());
        assert(pos_inf.construct_posinfkey());

        fake_shpid = 0;
        fake_pid = lsn_t(table_iter::_file->fid(), fake_shpid);
        fake_slot = 0;
    }

    ~table_scan_iter_impl() {
        tuple_iter_t<TableDesc, scan_file_i, table_row_t >::close_scan();
    }


    w_rc_t open_scan(ss_m* db) {
        if (!table_iter::_opened) {
            assert (db);
            table_iter::_scan = new scan_index_i(table_iter::_file->fid(),
                    cmp_t::ge, // open index on negative infinity for full scan
                    neg_inf,
                    cmt_t::le, // .. and close on positive infinity
                    pos_inf
            );
            table_iter::_opened = true;
        }
        return (RCOK);
    }

    w_rc_t next(ss_m* db, bool& eof, table_row_t& tuple)
    {
        assert (_pmanager);
        if (!table_iter::_opened) open_scan(db);

        pin_i* handle;
        W_DO(table_iter::_scan->next(eof));
        if (!eof) {
            /*
             * CS: TODO
             * For now, we assume that the key is the (fake) RID, and the
             * tuple is stored entirely in the value. Later, we may want
             * to use primary keys instead.
             */
            char* elem = table_iter::_scan->elem();
            if (!_pmanager->load(&tuple, data))
                return RC(se_WRONG_DISK_DATA);

            /*
             *
             * Problem: rid_t is from Shore, defined for heapfiles only
             * Solution: use fake RIDs, always incrementing slot id until
             * it overflows. Then increment page ID.
             */
            tuple.set_rid(rid_t(fake_pid, fake_slot++));
            if (fake_slot <= 0) { //overflow
                fake_slot = 0;
                fake_pid = lpid_t(table_iter::_file->fid(), fakeshpid++);
            }
        }

        return (RCOK);
    }

};
#endif // 0

#endif
