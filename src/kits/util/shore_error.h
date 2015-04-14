#ifndef UTIL_H
#define UTIL_H

#ifdef USE_SHORE
/* error codes returned from shore toolkit
 * CS: used to be in src/sm/shore_error.h in Kits
 */

enum {
  se_NOT_FOUND                = 0x810001,
  se_VOLUME_NOT_FOUND         = 0X810002,
  se_INDEX_NOT_FOUND          = 0x810003,
  se_TABLE_NOT_FOUND          = 0x810004,
  se_TUPLE_NOT_FOUND          = 0x810005,
  se_NO_CURRENT_TUPLE         = 0x810006,
  se_CANNOT_INSERT_TUPLE      = 0x810007,
  
  se_SCAN_OPEN_ERROR          = 0x810010,
  se_INCONSISTENT_INDEX       = 0x810012,
  se_OPEN_SCAN_ERROR          = 0x810020,
  
  se_LOAD_NOT_EXCLUSIVE       = 0x810040,
  se_ERROR_IN_LOAD            = 0x810041,
  se_ERROR_IN_IDX_LOAD        = 0x810042,

  se_WRONG_DISK_DATA          = 0x810050,

  se_INVALID_INPUT            = 0x810060
};
#endif

#endif
