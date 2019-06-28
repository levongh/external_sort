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
    using BlockPool = typename BlockMemoryAllocator<Block>::BlockPool;

    // Stream Types
    using IStream = InputStream<Block,
                                FileReader<Block>,
                                BlockMemoryAllocator<Block>>;

    using OStream = OutputStream<Block,
                                 FileWriter<Block>,
                                 BlockMemoryAllocator<Block>>;

    using IStreamPtr = std::shared_ptr<IStream>;
    using OStreamPtr = std::shared_ptr<OStream>;
};

template <typename ValueType>
typename Types<ValueType>::OStreamPtr
sort_and_write(typename Types<ValueType>::BlockPtr block,
               typename Types<ValueType>::OStreamPtr ostream)
{
    std::sort(block->begin(), block->end(), typename std::less<ValueType>());
    ostream->writeBlock(block);
    return ostream;
}

}
