#pragma once

#include <string>
#include <fstream>
#include <cstdio>

#include "block_types.h"

namespace external_sort
{

template <typename Block>
class FileReader
{
public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using ValueType = typename BlockTraits<Block>::ValueType;

    /// Policy interface
    void Open()
    {
        ifs_.open(input_filename_, std::ifstream::in /*| std::ifstream::binary*/);
    }

    void Close();
    void Read(BlockPtr& block);
    bool Empty() const;

    /// Set/get properties
    void set_input_filename(const std::string& ifn){ input_filename_ = ifn; }
    const std::string& input_filename() const { return input_filename_; }

    void set_input_rm_file(bool rm) { input_rm_file_ = rm; }
    bool input_rm_file() const { return input_rm_file_; }

private:
    std::ifstream ifs_;
    std::string input_filename_;
    bool input_rm_file_ = {false};
    size_t block_cnt_ = 0;
};

template <typename Block>
void FileReader<Block>::Close()
{
    if (ifs_.is_open()) {
        ifs_.close();
        if (input_rm_file_) {
            remove(input_filename_.c_str());
        }
    }
}

template <typename Block>
void FileReader<Block>::Read(BlockPtr& block)
{
    block->resize(block->capacity());
    std::streamsize bsize = block->size() * sizeof(ValueType);

    ifs_.read(reinterpret_cast<char*>(block->data()), bsize);
    if (ifs_.gcount() < bsize) {
        block->resize(ifs_.gcount() / sizeof(ValueType));
    }
    block_cnt_++;
}

template <typename Block>
bool FileReader<Block>::Empty() const
{
    return !(ifs_.is_open() && ifs_.good());
}

}
