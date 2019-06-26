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
    void setFilename(const std::string& ofn)
    {
        m_fileName = ofn;
    }

    const std::string& getFilename() const
    {
        return m_fileName;
    }

private:
    std::string m_fileName;
    std::ofstream ofs_;
};

template <typename Block>
void FileWriter<Block>::Open()
{
   ofs_.open(m_fileName, std::ofstream::out/*| std::ofstream::binary*/);
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


