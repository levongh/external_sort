#pragma once

#include <iomanip>
#include <sstream>
#include <unordered_set>

namespace external_sort {

enum MemUnit
{
    MB,
    KB,
    B
};

struct MemParams
{
    size_t  size   = 10;
    MemUnit unit   = MB;
    size_t  blocks = 2;
};

struct SplitParams
{
    MemParams mem;
    struct {
        std::string ifile;
        std::string ofile;
        bool rm_input = false;
    } spl;
    struct {
        std::list<std::string> ofiles;
    } out;
};

struct MergeParams
{
    MemParams mem;
    struct {
        size_t merges    = 4;
        size_t kmerge    = 4;
        size_t stmblocks = 2;
        std::list<std::string> ifiles;
        std::string tfile;
        std::string ofile;
        bool rm_input = true;
    } mrg;
};

struct GenerateParams
{
    MemParams mem;
    struct {
        size_t fsize = 0;
        std::string ofile;
    } gen;
};

//! Default generator
template <typename T>
struct Generator
{
    T operator()()
    {
        union {
            T data;
            uint8_t bytes[sizeof(T)];
        } u;
        for (auto& b : u.bytes) {
            b = rand() & 0xFF;
        }
        return u.data;
    }
};

const char* DEF_SPL_TMP_SFX = "split";
const char* DEF_MRG_TMP_SFX = "merge";

template <typename SizeType>
SizeType memsize_in_bytes(const SizeType& memsize, const MemUnit& u)
{
    if (u == KB) {
        return memsize << 10;
    }
    if (u == MB) {
        return memsize << 20;
    }
    return memsize;
}

template <typename IndexType>
std::string createFileName(const std::string& prefix,
                              const std::string& suffix,
                              const IndexType& index)
{
    std::ostringstream filename;
    filename << prefix << "." << suffix << "."
             << std::setfill ('0') << std::setw(3) << index;
    return filename.str();
}

} // namespace external_sort
