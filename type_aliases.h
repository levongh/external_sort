#pragma once

#include "policy/memory_policy.h"
#include "policy/read_policy.h"
#include "policy/write_policy.h"
#include "input_stream.h"
#include "output_stream.h"
#include "policy/block_types.h"
#include "utility.h"

#include <algorithm>
#include <memory>

namespace external_sort {

//! All types in one place
template <typename ValueType>
struct Types
{
    // Value trait shortcuts
    using Comparator = typename ValueTraits<ValueType>::Comparator;

    // Block Types
    using Block = VectorBlock<ValueType>;
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using BlockPool = typename BlockMemoryPolicy<Block>::BlockPool;
    //using BlockTraits = typename BlockTraits<Block>;

    // Stream Types
    using IStream = BlockInputStream<Block,
                                     BlockFileReadPolicy<Block>,
                                     BlockMemoryPolicy<Block>>;

    using OStream = BlockOutputStream<Block,
                                             BlockFileWritePolicy<Block>,
                                             BlockMemoryPolicy<Block>>;

    using IStreamPtr = std::shared_ptr<IStream>;
    using OStreamPtr = std::shared_ptr<OStream>;
};

template <typename ValueType>
typename Types<ValueType>::OStreamPtr
sort_and_write(typename Types<ValueType>::BlockPtr block,
               typename Types<ValueType>::OStreamPtr ostream)
{
    std::sort(block->begin(), block->end(), typename Types<ValueType>::Comparator());
    ostream->WriteBlock(block);
    return ostream;
}

}
