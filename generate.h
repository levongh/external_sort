#pragma once

#include "util/block_types.h"
#include "util/output_stream.h"
#include "util/utility.h"


//! External Generate
template <typename ValueType>
void generate(const external_sort::GenerateParams& params)
{
    using namespace external_sort;
    auto generator = typename ValueTraits<ValueType>::Generator();
    size_t gen_elements = memsize_in_bytes(params.gen.fsize, params.mem.unit) /
        sizeof(ValueType);

    auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
    ostream->set_mem_pool(memsize_in_bytes(params.mem.size, params.mem.unit),
            params.mem.blocks);
    ostream->set_output_filename(params.gen.ofile);
    ostream->Open();

    for (size_t i = 0; i < gen_elements; i++) {
        ostream->Push(generator());
    }

    ostream->Close();
}
