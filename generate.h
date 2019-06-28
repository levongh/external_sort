#pragma once

#include "util/outputstream.h"
#include "util/utility.h"

template <typename ValueType>
void generate(const external_sort::GenerateParams& params)
{
    using namespace external_sort;
    auto generator = Generator<ValueType>();
    size_t gen_elements = memsize_in_bytes(params.gen.fsize, params.mem.unit) /
        sizeof(ValueType);

    auto ostream = std::make_shared<OutputStream<std::vector<ValueType> > >();
    ostream->setPool(memsize_in_bytes(params.mem.size, params.mem.unit),
            params.mem.blocks);
    ostream->setFilename(params.gen.ofile);
    ostream->open();

    for (size_t i = 0; i < gen_elements; i++) {
        ostream->push(generator());
    }

    ostream->close();
}
