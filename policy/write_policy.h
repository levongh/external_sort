#pragma once

#include <string>
#include <queue>
#include <fstream>

#include "block_types.h"

namespace external_sort {

template <typename Block>
class BlockFileWritePolicy
{
public:
    using BlockPtr = typename BlockTraits<Block>::BlockPtr;
    using ValueType = typename BlockTraits<Block>::ValueType;

    /// Policy interface
    void Open();
    void Close();
    void Write(const BlockPtr& block);

    /// Set/get properties
    void set_output_filename(const std::string& ofn)
    {
        output_filename_ = ofn;
    }

    const std::string& output_filename() const
    {
        return output_filename_;
    }

private:
    void FileOpen();
    void FileWrite(const BlockPtr& block);
    void FileClose();

private:
    std::string output_filename_;
    std::ofstream ofs_;
};

template <typename Block>
void BlockFileWritePolicy<Block>::Open()
{
    FileOpen();
}

template <typename Block>
void BlockFileWritePolicy<Block>::Close()
{
    FileClose();
}

template <typename Block>
void BlockFileWritePolicy<Block>::Write(const BlockPtr& block)
{
    if (!block || block->empty()) {
        return;
    }

    FileWrite(block);
}

template <typename Block>
void BlockFileWritePolicy<Block>::FileOpen()
{
   ofs_.open(output_filename_, std::ofstream::out/*| std::ofstream::binary*/);
    if (!ofs_) {
        //LOG_ERR(("Failed to open output file: %s") % output_filename_);
    }
}

template <typename Block>
void BlockFileWritePolicy<Block>::FileWrite(const BlockPtr& block)
{
    ofs_.write((const char*)block->data(), block->size() * sizeof(ValueType));
}

template <typename Block>
void BlockFileWritePolicy<Block>::FileClose()
{
    if (ofs_.is_open()) {
        ofs_.close();
    }
}

} // namespace external_sort


