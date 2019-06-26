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
    size_t  size   = 10;                // memory size
    MemUnit unit   = MB;                // memory unit
    size_t  blocks = 2;                 // number of blocks memory is divided by
};

struct SplitParams
{
    MemParams mem;                      // memory params
    struct {
        std::string ifile;              // input file to split
        std::string ofile;              // output file prefix (prefix of splits)
        bool rm_input = false;          // ifile should be removed when done?
    } spl;
    struct {
        std::list<std::string> ofiles;  // list of output files (splits)
    } out;
};

struct MergeParams
{
    MemParams mem;                      // memory params
    struct {
        size_t merges    = 4;           // number of simultaneous merges
        size_t kmerge    = 4;           // number of streams to merge at a time
        size_t stmblocks = 2;           // number of memory blocks per stream
        std::list<std::string> ifiles;  // list of input files to merge
        std::string tfile;              // prefix for temporary files
        std::string ofile;              // output file (the merge result)
        bool rm_input = true;           // ifile should be removed when done?
    } mrg;
};

struct GenerateParams
{
    MemParams mem;                      // memory params
    struct {
        size_t fsize = 0;               // file size to generate (in mem.units)
        std::string ofile;              // output file
    } gen;
};

//! Default generator
template <typename T>
struct DefaultValueGenerator
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

//! Default value-to-string convertor
template <typename ValueType>
struct DefaultValue2Str
{
    std::string operator()(const ValueType& value)
    {
        std::ostringstream ss;
        ss << value;
        return ss.str();
    }
};

//! Default ValueType traits
template <typename ValueType>
struct ValueTraits
{
    using Comparator = std::less<ValueType>;
    using Generator = DefaultValueGenerator<ValueType>;
    using Value2Str = DefaultValue2Str<ValueType>;
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
std::string make_tmp_filename(const std::string& prefix,
                              const std::string& suffix,
                              const IndexType& index)
{
    std::ostringstream filename;
    filename << prefix << "." << suffix << "."
             << std::setfill ('0') << std::setw(3) << index;
    return filename.str();
}

} // namespace external_sort
