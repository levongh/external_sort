#include <iostream>
#include "split.h"
#include "generate.h"
#include "merge.h"

using ValueType = double;

int main()
{

    external_sort::GenerateParams params;
    params.mem.size   = 10;
    params.mem.unit   = external_sort::MB;
//    params.mem.blocks = vm["gen.blocks"].as<size_t>();
    params.gen.ofile  = "big_input_file";
    params.gen.fsize  = 10;

    generate<ValueType>(params);

    // set split and merge parameters
    external_sort::SplitParams sp;
    external_sort::MergeParams mp;
    sp.mem.size = 10;
    sp.mem.unit = external_sort::MB;
    mp.mem = sp.mem;
    sp.spl.ifile = "big_input_file";
    mp.mrg.ofile = "big_sorted_file";

    sort<ValueType>(sp, mp);

    if (sp.err.none && mp.err.none) {
        std::cout << "File sorted successfully!" << std::endl;
    } else {
        std::cout << "External sort failed!" << std::endl;
        if (sp.err) {
            std::cout << "Split failed: " << sp.err.msg() << std::endl;
        } else {
            std::cout << "Merge failed: " << mp.err.msg() << std::endl;
        }
    }
}
