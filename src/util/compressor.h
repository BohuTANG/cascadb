#ifndef CASCADB_UTIL_COMPRESSOR_H_
#define CASCADB_UTIL_COMPRESSOR_H_

#include "cascadb/options.h"
#include <snappy.h>
#include <quicklz.h>

namespace cascadb {

class Compressor {
public:
    Compressor(enum Compress method): method_(method)
    {
        qsc_ = (qlz_state_compress*)calloc(1, sizeof(*qsc_));
        qsd_ = (qlz_state_decompress*)calloc(1, sizeof(*qsd_));
    }

    ~Compressor()
    {
        free(qsc_);
        free(qsd_);
    }

    size_t max_compressed_length(size_t size);

    bool compress(const char *ibuf, size_t size, char *obuf, size_t *sp);

    // obuf should be larger than uncompressed length
    bool uncompress(const char *ibuf, size_t size, char *obuf);

private:
    enum Compress method_;
    qlz_state_compress *qsc_;
    qlz_state_decompress *qsd_;
};
}

#endif
