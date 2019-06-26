#pragma once

#include <string>
#include <fstream>
#include <cstdio>

namespace external_sort
{

template <typename Block>
class FileReader
{
public:
    /// Policy interface
    void Open()
    {
        m_inputStream.open(m_fileName, std::ifstream::in /*| std::ifstream::binary*/);
    }

    void Close();
    void Read(Block*& block);
    bool Empty() const;

    /// Set/get properties
    void setFilename(const std::string& ifn)
    {
        m_fileName = ifn;
    }

    void setFileRM(bool rm)
    {
        m_rmFile = rm;
    }

private:
    bool m_rmFile = {false};
    std::ifstream m_inputStream;
    std::string m_fileName;
};

template <typename Block>
void FileReader<Block>::Close()
{
    if (m_inputStream.is_open()) {
        m_inputStream.close();
        if (m_rmFile) {
            remove(m_fileName.c_str());
        }
    }
}

template <typename Block>
void FileReader<Block>::Read(Block*& block)
{
    block->resize(block->capacity());
    std::streamsize bsize = block->size() * sizeof(typename Block::value_type);

    m_inputStream.read(reinterpret_cast<char*>(block->data()), bsize);
    if (m_inputStream.gcount() < bsize) {
        block->resize(m_inputStream.gcount() / sizeof(typename Block::value_type));
    }
}

template <typename Block>
bool FileReader<Block>::Empty() const
{
    return !(m_inputStream.is_open() && m_inputStream.good());
}

}
