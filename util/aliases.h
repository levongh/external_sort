#pragma once

#include <algorithm>
#include <memory>

#include "allocator.h"
#include "filereader.h"
#include "filewriter.h"
#include "inputstream.h"
#include "outputstream.h"
#include "utility.h"


namespace external_sort {

namespace detail {

template <typename T>
using Block = std::vector<T>;

template <typename T>
using Ptr = std::shared_ptr<T>;

template <typename T>
using IStreamPtr = Ptr<InputStream<Block<T> > >;

template <typename T>
using OStreamPtr = Ptr<OutputStream<Block<T> > >;


template <typename T>
using BlockPool = typename Allocator<Block<T> >::BlockPool;

}
template <typename ValueType>
detail::OStreamPtr<ValueType>sort_and_write(
               detail::Block<ValueType>* block,
               detail::OStreamPtr<ValueType> ostream)
{
    std::sort(block->begin(), block->end(), typename std::less<ValueType>());
    ostream->writeBlock(block);
    return ostream;
}

}
