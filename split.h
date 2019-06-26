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
    istream->set_input_filename(params.spl.ifile);
    istream->set_input_rm_file(params.spl.rm_input);
    istream->Open();

    if (params.spl.ofile.empty()) {
        params.spl.ofile = params.spl.ifile;
    }

    while (!istream->Empty()) {
        auto block = istream->FrontBlock();
        istream->PopBlock();

        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(mem_pool);
        ostream->set_output_filename(
            make_tmp_filename(params.spl.ofile, DEF_SPL_TMP_SFX, ++file_cnt));
        ostream->Open();

        splits.Async(&sort_and_write<ValueType>,
                     std::move(block), std::move(ostream));

        while ((splits.Ready() > 0) || (splits.Running() && istream->Empty())) {
            auto ostream_ready = splits.get();
            if (ostream_ready) {
                ostream_ready->Close();
                params.out.ofiles.push_back(ostream_ready->output_filename());
            }
        }
    }
    istream->Close();
}


