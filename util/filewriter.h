#pragma once

#include <string>
#include <queue>
#include <fstream>

namespace external_sort {

template <typename Block>
class FileWriter
{
public:
    using BlockPtr = Block*;
    using ValueType = typename Block::value_type;

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
    std::string output_filename_;
    std::ofstream ofs_;
};

template <typename Block>
void FileWriter<Block>::Open()
{
   ofs_.open(output_filename_, std::ofstream::out/*| std::ofstream::binary*/);
}

template <typename Block>
void FileWriter<Block>::Close()
{
    if (ofs_.is_open()) {
        ofs_.close();
    }
}

template <typename Block>
void FileWriter<Block>::Write(const BlockPtr& block)
{
    if (!block || block->empty()) {
        return;
    }

    ofs_.write((const char*)block->data(), block->size() * sizeof(ValueType));
}

} // namespace external_sort


