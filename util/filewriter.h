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
    void open();
    void close();
    void write(const BlockPtr& block);

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
    std::ofstream m_stream;
};

template <typename Block>
void FileWriter<Block>::open()
{
   m_stream.open(m_fileName, std::ofstream::out | std::ofstream::binary);
}

template <typename Block>
void FileWriter<Block>::close()
{
    if (m_stream.is_open()) {
        m_stream.close();
    }
}

template <typename Block>
void FileWriter<Block>::write(const BlockPtr& block)
{
    if (!block || block->empty()) {
        return;
    }
    m_stream.write((const char*)block->data(), block->size() * sizeof(ValueType));
}

} // namespace external_sort


