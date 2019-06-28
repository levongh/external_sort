
template <typename Block>
class BlockMemoryAllocator<Block>::BlockPool
{
public:
    BlockPool(size_t memsize, size_t memblocks)
        : m_blocks(memblocks)
          , m_blocksAllocated(0)
    {
        size_t block_size = memsize / memblocks /
            (sizeof(typename Block::value_type));

        while (m_pool.size() < m_blocks) {
            Block* block(new Block);
            block->reserve(block_size);
            m_pool.push(block);
        }
    }

    ~BlockPool()
    {
        while (!m_pool.empty()) {
            Block* block = m_pool.top();
            delete block;
            m_pool.pop();
        }
    }

public:
    size_t allocated() const
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        return m_blocksAllocated;
    }

    Block* allocate()
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        while (m_pool.empty()) {
            m_cv.wait(lck);
        }
        Block* block = m_pool.top();
        m_pool.pop();
        ++m_blocksAllocated;
        return block;
    }

    void free(Block* block)
    {
        std::unique_lock<std::mutex> lck(m_mutex);
        --m_blocksAllocated;
        block->resize(0);
        m_pool.push(block);
        m_cv.notify_one();
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    std::stack<Block*> m_pool;
    size_t m_blocks;
    size_t m_blocksAllocated;
};
