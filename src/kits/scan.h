#ifndef KITS_SCAN_H
#define KITS_SCAN_H

#include "table_man.h"

template <class TableDesc>
class table_scan_iter_impl
{
private:
    table_man_t<TableDesc>* _pmanager;
    bt_cursor_t* btcursor;

    static const slotid_t max_slot;

public:

    table_scan_iter_impl(TableDesc* ptable,
                         table_man_t<TableDesc>* pmanager)
        : _pmanager(pmanager), btcursor(NULL)
    {
        assert (_pmanager);
        W_COERCE(open_scan());
    }

    ~table_scan_iter_impl() {
        delete btcursor;
    }


    w_rc_t open_scan() {
        if (!btcursor) {
            btcursor = new bt_cursor_t(
                    _pmanager->table()->primary_idx()->stid(),
                    true /* forward */);
        }
        return (RCOK);
    }

    w_rc_t next(bool& eof, table_row_t& tuple)
    {
        assert (_pmanager);
        if (!btcursor) open_scan();

        if (btcursor->eof()) {
            eof = true;
            return RCOK;
        }

        W_DO(btcursor->next());
        bool loaded;

        // Load key
        btcursor->key().serialize_as_nonkeystr(tuple._rep_key->_dest);
        loaded = _pmanager->load_key(tuple._rep_key->_dest,
                _pmanager->table()->primary_idx(), &tuple);
        w_assert0(loaded);

        // Load element
        char* elem = btcursor->elem();
        loaded = _pmanager->load(&tuple, elem, _pmanager->table()->primary_idx());
        w_assert0(loaded);

        return (RCOK);
    }

};

template <class TableDesc>
class index_scan_iter_impl
{
private:
    table_man_t<TableDesc>* _pmanager;
    index_desc_t* _pindex;
    bt_cursor_t* btcursor;
    bool           _need_tuple;

public:

    /* -------------------- */
    /* --- construction --- */
    /* -------------------- */

    index_scan_iter_impl(index_desc_t* pindex,
                         table_man_t<TableDesc>* pmanager,
                         bool need_tuple = false)
          : _pmanager(pmanager), _pindex(pindex), btcursor(NULL),
          _need_tuple(need_tuple)
    {
        assert (_pmanager);
        assert (_pindex);
    }

    ~index_scan_iter_impl() {
        if (btcursor) {
            delete btcursor;
        }
    };

    w_rc_t open_scan(char* lower, int lowsz, bool lower_incl,
                     char* upper, int upsz, bool upper_incl)
    {
        if (!btcursor) {
            w_keystr_t kup, klow;
            kup.construct_regularkey(upper, upsz);
            klow.construct_regularkey(lower, lowsz);
            btcursor = new bt_cursor_t(
                    _pindex->stid(),
                    klow, lower_incl, kup, upper_incl, true /*forward*/);
        }

        return (RCOK);
    }

    w_rc_t next(bool& eof, table_row_t& tuple)
    {
        assert (_pmanager);
        assert (btcursor);

        if (btcursor->eof()) {
            eof = true;
            return RCOK;
        }

        W_DO(btcursor->next());
        bool loaded;

        // Load key
        btcursor->key().serialize_as_nonkeystr(tuple._rep_key->_dest);
        loaded = _pmanager->load_key(tuple._rep_key->_dest,
                _pindex, &tuple);
        w_assert0(loaded);

        if (_need_tuple) {
            // Fetch tuple from primary index
            char* fkey = btcursor->elem();
            smsize_t elen = btcursor->elen();
            w_keystr_t fkeystr;
            fkeystr.construct_regularkey(fkey, elen);

            ss_m::find_assoc(_pmanager->table()->primary_idx()->stid(),
                    fkeystr, tuple._rep->_dest, elen, loaded);
            w_assert0(loaded);

            loaded = _pmanager->load(&tuple, tuple._rep->_dest);
            w_assert0(loaded);
        }
        return (RCOK);
    }

};

#endif
