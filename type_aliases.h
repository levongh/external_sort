#pragma once

#include "util/allocator.h"
#include "util/filereader.h"
#include "util/filewriter.h"
#include "util/inputstream.h"
#include "util/outputstream.h"
#include "util/utility.h"

#include <algorithm>
#include <memory>

namespace external_sort {

//! All types in one place
template <typename ValueType>
struct Types
{
    // Block Types
    using Block = std::vector<ValueType>;
    using BlockPtr = Block*;
    using BlockPool = typename Allocator<Block>::BlockPool;

    using IStreamPtr = std::shared_ptr<InputStream<Block> >;
    using OStreamPtr = std::shared_ptr<OutputStream<Block> >;
};

template <typename ValueType>
std::shared_ptr<OutputStream<std::vector<ValueType> > >
sort_and_write(
               std::vector<ValueType>* block,
               std::shared_ptr<OutputStream<std::vector<ValueType> > > ostream)
{
    std::sort(block->begin(), block->end(), typename std::less<ValueType>());
    ostream->writeBlock(block);
    return ostream;
}

}
