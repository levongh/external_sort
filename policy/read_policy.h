#pragma once

#include <string>
#include <fstream>
#include <cstdio>

#include "block_types.h"

namespace external_sort
{

template <typename Block>
class BlockFileReadPolicy
{
public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using ValueType = typename BlockTraits<Block>::ValueType;

    /// Policy interface
    void Open();
    void Close();
    void Read(BlockPtr& block);
    bool Empty() const;

    /// Set/get properties
    void set_input_filename(const std::string& ifn){ input_filename_ = ifn; }
    const std::string& input_filename() const { return input_filename_; }

    void set_input_rm_file(bool rm) { input_rm_file_ = rm; }
    bool input_rm_file() const { return input_rm_file_; }

private:
    void FileOpen();
    void FileRead(BlockPtr& block);
    void FileClose();

private:
    std::ifstream ifs_;
    std::string input_filename_;
    bool input_rm_file_ = {false};
    size_t block_cnt_ = 0;
};

template <typename Block>
void BlockFileReadPolicy<Block>::Open()
{
    FileOpen();
}

template <typename Block>
void BlockFileReadPolicy<Block>::Close()
{
    FileClose();
}

template <typename Block>
void BlockFileReadPolicy<Block>::Read(BlockPtr& block)
{
    FileRead(block);
    block_cnt_++;
}

template <typename Block>
bool BlockFileReadPolicy<Block>::Empty() const
{
    return !(ifs_.is_open() && ifs_.good());
}

template <typename Block>
void BlockFileReadPolicy<Block>::FileOpen()
{
    ifs_.open(input_filename_, std::ifstream::in /*| std::ifstream::binary*/);
    if (!ifs_) {
        //LOG_ERR(("Failed to open input file: %s") % input_filename_);
    }
}

template <typename Block>
void BlockFileReadPolicy<Block>::FileRead(BlockPtr& block)
{
    block->resize(block->capacity());
    std::streamsize bsize = block->size() * sizeof(ValueType);

    ifs_.read(reinterpret_cast<char*>(block->data()), bsize);
    if (ifs_.gcount() < bsize) {
        block->resize(ifs_.gcount() / sizeof(ValueType));
    }
}

template <typename Block>
void BlockFileReadPolicy<Block>::FileClose()
{
    if (ifs_.is_open()) {
        ifs_.close();
        if (input_rm_file_) {
            if (remove(input_filename_.c_str()) != 0) {
                //LOG_ERR(("Failed to remove file: %s") % input_filename_);
            }
        }
    }
}

}
