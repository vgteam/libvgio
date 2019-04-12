#ifndef VG_IO_HFILE_INTERNAL_HPP_INCLUDED
#define VG_IO_HFILE_INTERNAL_HPP_INCLUDED

/**
 * \file hfile_internal.hpp
 * Contains minimal definitions required to implement an htslib hFILE_backend.
 * See https://github.com/samtools/htslib/blob/master/hfile_internal.h for field and argument documentation.
 * Can be deleted when https://github.com/samtools/htslib/issues/849 is fixed.
 */
 
extern "C" {
 
struct hFILE;
 
struct hFILE_backend {
    ssize_t (*read)(hFILE*, void*, size_t);
    ssize_t (*write)(hFILE*, const void*, size_t);
    off_t (*seek)(hFILE*, off_t, int);
    int (*flush)(hFILE*);
    int (*close)(hFILE*);
};

hFILE* hfile_init(size_t, const char*, size_t);

}

#endif
