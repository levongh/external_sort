#pragma once

#include "async_funcs.h"
#include "type_aliases.h"
#include "util/output_stream.h"

//! External Split
template <typename ValueType>
void split(external_sort::SplitParams& params)
{
    using namespace external_sort;
    size_t file_cnt = 0;

    external_sort::AsyncFuncs<typename Types<ValueType>::OStreamPtr> splits;

    auto mem_pool = std::make_shared<typename Types<ValueType>::BlockPool>(
        memsize_in_bytes(params.mem.size, params.mem.unit), params.mem.blocks);

    auto istream = std::make_shared<typename Types<ValueType>::IStream>();
    istream->set_mem_pool(mem_pool);
    istream->setFilename(params.spl.ifile);
    istream->setFileRM(params.spl.rm_input);
    istream->open();

    if (params.spl.ofile.empty()) {
        params.spl.ofile = params.spl.ifile;
    }

    while (!istream->empty()) {
        auto block = istream->block();
        istream->null();

        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(mem_pool);
        ostream->setFilename(
            make_tmp_filename(params.spl.ofile, DEF_SPL_TMP_SFX, ++file_cnt));
        ostream->open();

        splits.addTask(&sort_and_write<ValueType>,
                     std::move(block), std::move(ostream));

        while ((splits.ready() > 0) || (splits.running() && istream->empty())) {
            auto ostream_ready = splits.get();
            if (ostream_ready) {
                ostream_ready->close();
                params.out.ofiles.push_back(ostream_ready->getFilename());
            }
        }
    }
    istream->close();
}


