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

    // Merge files while there is something to merge or there are ongoing merges
    auto files = params.mrg.ifiles;
    while (files.size() > 1 || !merges.Empty()) {
        // create a set of input streams with next kmerge files from the queue
        std::unordered_set<typename Types<ValueType>::IStreamPtr> istreams;
        while (istreams.size() < params.mrg.kmerge && !files.empty()) {
            // create input stream
            auto is = std::make_shared<typename Types<ValueType>::IStream>();
            is->set_mem_pool(mem_istream, params.mrg.stmblocks);
            is->set_input_filename(files.front());
            is->set_input_rm_file(params.mrg.rm_input);
            // add to the set
            istreams.insert(is);
            files.pop_front();
        }

        // create an output stream
        auto ostream = std::make_shared<typename Types<ValueType>::OStream>();
        ostream->set_mem_pool(mem_ostream, params.mrg.stmblocks);
        ostream->set_output_filename(make_tmp_filename(
            (params.mrg.tfile.size() ? params.mrg.tfile : params.mrg.ofile),
            DEF_MRG_TMP_SFX, ++file_cnt));

        // asynchronously merge and write to the output stream
        merges.Async(&merge_streams<typename Types<ValueType>::IStreamPtr,
                                    typename Types<ValueType>::OStreamPtr>,
                     std::move(istreams), std::move(ostream));

        // Wait/get results of asynchroniously running merges if:
        // 1) Too few files ready to be merged, while still running merges.
        //    In other words, more files can be merged at once than
        //    currently available. So wait for more files.
        // 2) There are completed (ready) merges; results shall be collected
        // 3) There are simply too many already ongoing merges
        while ((files.size() < params.mrg.kmerge && !merges.Empty()) ||
               (merges.Ready() > 0) || (merges.Running() >= params.mrg.merges)) {
            auto ostream_ready = merges.get();
            if (ostream_ready) {
                files.push_back(ostream_ready->output_filename());
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

