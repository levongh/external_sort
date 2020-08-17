#pragma once

#include "tasksheduler.h"
#include "util/aliases.h"
#include "util/outputstream.h"

//! External Split
template <typename ValueType>
void split(external_sort::SplitParams& params)
{
    using namespace external_sort;
    using namespace detail;

    const static std::string TMP_SUFFIX = "split";
    size_t file_cnt = 0;

    TaskSheduler<OStreamPtr<ValueType> > splits;

    auto mem_pool = std::make_shared<BlockPool<ValueType> >(
        memsize_in_bytes(params.mem.size, params.mem.unit), params.mem.blocks);

    auto istream = std::make_shared<InputStream<std::vector<ValueType> > >();
    istream->setPool(mem_pool);
    istream->setFilename(params.spl.ifile);
    istream->setFileRM(params.spl.rm_input);
    istream->open();

    if (params.spl.ofile.empty()) {
        params.spl.ofile = params.spl.ifile;
    }

    while (!istream->empty()) {
        auto block = istream->block();
        istream->null();

        auto ostream = std::make_shared<OutputStream<std::vector<ValueType> > >();
        ostream->setPool(mem_pool);
        ostream->setFilename(
            createFileName(params.spl.ofile, TMP_SUFFIX, ++file_cnt));
        ostream->open();

        splits.shedule(&sort_and_write<ValueType>,
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


