#include <iostream>
#include "split.h"
#include "generate.h"
#include "merge.h"

int main()
{
    external_sort::GenerateParams params;
    params.mem.size   = 10;
    params.mem.unit   = external_sort::MB;
    params.gen.ofile  = "big_input_file";
    params.gen.fsize  = 10;

    generate<double>(params);

    // set split and merge parameters
    external_sort::SplitParams sp;
    external_sort::MergeParams mp;
    sp.mem.size = 10;
    sp.mem.unit = external_sort::MB;
    mp.mem = sp.mem;
    sp.spl.ifile = "big_input_file";
    mp.mrg.ofile = "big_sorted_file";

    sort<double>(sp, mp);

    std::cout << "File sorted successfully!" << std::endl;
    return 0;
}
