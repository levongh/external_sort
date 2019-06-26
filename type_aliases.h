#pragma once

#include "util/memory_policy.h"
#include "util/filereader.h"
#include "util/filewriter.h"
#include "util/input_stream.h"
#include "util/output_stream.h"
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
    using BlockPool = typename BlockMemoryPolicy<Block>::BlockPool;

    // Stream Types
    using IStream = BlockInputStream<Block,
                                     FileReader<Block>,
                                     BlockMemoryPolicy<Block>>;

    using OStream = BlockOutputStream<Block,
                                             FileWriter<Block>,
                                             BlockMemoryPolicy<Block>>;

    using IStreamPtr = std::shared_ptr<IStream>;
    using OStreamPtr = std::shared_ptr<OStream>;
};

template <typename ValueType>
typename Types<ValueType>::OStreamPtr
sort_and_write(typename Types<ValueType>::BlockPtr block,
               typename Types<ValueType>::OStreamPtr ostream)
{
    std::sort(block->begin(), block->end(), typename std::less<ValueType>());
    ostream->WriteBlock(block);
    return ostream;
}

}
