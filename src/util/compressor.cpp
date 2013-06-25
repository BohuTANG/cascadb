#include "logger.h"
#include "compressor.h"

using namespace std;
using namespace cascadb;

#include "quicklz.h"
#include "snappy.h"

size_t Compressor::max_compressed_length(size_t size)
{
    size_t s = 0;

    switch (method_) {
        case kNoCompress:
           s = size + 1;
           break;
        case kSnappyCompress:
           s = snappy::MaxCompressedLength(size) + 1;
           break;
        case kQuicklzCompress:
           s = size + 400 + 1;
           break;
        default: 
           break;
    }

    return s;
}

bool Compressor::compress(const char *ibuf, size_t size, char *obuf, size_t *sp)
{
    size_t olen;
    bool b= false;

    if (size == 0)
        return b;

    switch (method_) {
        case kNoCompress:
            obuf[0] = kNoCompress;
            memcpy(obuf + 1, ibuf, size);
            *sp = (size + 1);

            b = true;
            break;
        case kSnappyCompress:
            obuf[0] = kSnappyCompress;
            snappy::RawCompress(ibuf, size, obuf + 1, &olen);
            *sp = olen + 1;

            b = true;
            break;

        case kQuicklzCompress:
            obuf[0] = kQuicklzCompress;
            olen = qlz_compress(ibuf, obuf + 1, size, qsc_);
            *sp = olen + 1;

            b = true;
            break;

        default: 
            LOG_ERROR("no compress method support");
            break;
    }

    return b;
}

bool Compressor::uncompress(const char *ibuf, size_t size, char *obuf)
{
    bool b = false;

    if (size == 0)
        return b;

    switch (ibuf[0] & 0xF) {
        case kNoCompress:
            memcpy(obuf, ibuf + 1, size - 1);

            b = true;
            break;
        case kSnappyCompress:
            if (!snappy::RawUncompress(ibuf + 1, size - 1, obuf)) {
                LOG_ERROR("snappy uncompress error");
                break;
            }

            b = true;
            break;
        case kQuicklzCompress:
            if (size > 0) {
                qlz_decompress(ibuf + 1, obuf, qsd_);
            }

            b = true;
            break;

        default: 
            LOG_ERROR("no compress method support");
            break;
    }

    return b;
}
