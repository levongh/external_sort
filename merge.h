#pragma once

#include "async_funcs.h"
#include "util/output_stream.h"
#include "util/input_stream.h"
#include "util/utility.h"
#include "sort_merge.h"

template <typename ValueType>
void merge(external_sort::MergeParams& params)
{
    using namespace external_sort;
    size_t file_cnt = 0;

    AsyncFuncs<typename Types<ValueType>::OStreamPtr> merges;

    size_t mem_merge = memsize_in_bytes(params.mem.size, params.mem.unit) /
                       params.mrg.merges;
    size_t mem_ostream = mem_merge / 2;
    size_t mem_istream = mem_merge - mem_ostream;

    auto files = params.mrg.ifiles;
    while (files.size() > 1 || !merges.empty()) {
        std::unordered_set<typename Types<ValueType>::IStreamPtr> istreams;
        while (istreams.size() < params.mrg.kmerge && !files.empty()) {
            auto is = std::make_shared<typename Types<ValueType>::IStream>();
            is->setPool(mem_istream, params.mrg.stmblocks);
            is->setFilename(files.front());
            is->setFileRM(params.mrg.rm_input);
            istreams.insert(is);
            files.pop_front();
        }

        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->setPool(mem_ostream, params.mrg.stmblocks);
        ostream->setFilename(createFileName(
            (params.mrg.tfile.size() ? params.mrg.tfile : params.mrg.ofile),
            DEF_MRG_TMP_SFX, ++file_cnt));

        merges.addTask(&merge_streams<typename Types<ValueType>::IStreamPtr,
                                    typename Types<ValueType>::OStreamPtr>,
                     std::move(istreams), std::move(ostream));

        while ((files.size() < params.mrg.kmerge && !merges.empty()) ||
               (merges.ready() > 0) || (merges.running() >= params.mrg.merges)) {
            auto ostream_ready = merges.get();
            if (ostream_ready) {
                files.push_back(ostream_ready->getFilename());
            }
        }
    }

    if (files.size()) {
        rename(files.front().c_str(), params.mrg.ofile.c_str());
    }
}

//! External Sort (= Split + Merge)
template <typename ValueType>
void sort(external_sort::SplitParams& sp, external_sort::MergeParams& mp)
{
    split<ValueType>(sp);

    mp.mrg.ifiles = sp.out.ofiles;
    merge<ValueType>(mp);
}

