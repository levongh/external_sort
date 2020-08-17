#pragma once

#include "tasksheduler.h"
#include "util/outputstream.h"
#include "util/inputstream.h"
#include "util/utility.h"
#include "sort_merge.h"
#include "util/aliases.h"

template <typename ValueType>
void merge(external_sort::MergeParams& params)
{
    using namespace external_sort;
    using namespace detail;

    const static std::string TMP_SUFFIX = "merge";
    size_t file_cnt = 0;

    TaskSheduler<OStreamPtr<ValueType> > merges;

    size_t mem_merge = memsize_in_bytes(params.mem.size, params.mem.unit) /
                       params.mrg.merges;
    size_t mem_ostream = mem_merge / 2;
    size_t mem_istream = mem_merge - mem_ostream;

    auto files = params.mrg.ifiles;
    while (files.size() > 1 || !merges.empty()) {
        std::unordered_set<IStreamPtr<ValueType > > istreams;
        while (istreams.size() < params.mrg.kmerge && !files.empty()) {
            auto is = std::make_shared<InputStream<std::vector<ValueType> > >();
            is->setPool(mem_istream, params.mrg.stmblocks);
            is->setFilename(files.front());
            is->setFileRM(params.mrg.rm_input);
            istreams.insert(is);
            files.pop_front();
        }

        auto ostream = std::make_shared<OutputStream<std::vector<ValueType> > > ();
        ostream->setPool(mem_ostream, params.mrg.stmblocks);
        ostream->setFilename(createFileName(
            (params.mrg.tfile.size() ? params.mrg.tfile : params.mrg.ofile),
            TMP_SUFFIX, ++file_cnt));

        merges.shedule(&merge_streams<IStreamPtr<ValueType>,
                                      OStreamPtr<ValueType>>,
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

template <typename ValueType>
void sort(external_sort::SplitParams& sp, external_sort::MergeParams& mp)
{
    split<ValueType>(sp);

    mp.mrg.ifiles = sp.out.ofiles;
    merge<ValueType>(mp);
}

