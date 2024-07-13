#ifndef LOCKFREEQUEUE_H
#define LOCKFREEQUEUE_H

#include <atomic>
#include <vector>

template <typename T>
class LockFreeQueue {
public:
    explicit LockFreeQueue(size_t capacity)
        : buffer(capacity), head(0), tail(0), capacity(capacity) {}

    bool enqueue(const T& item) {
        size_t currentTail = tail.load(std::memory_order_relaxed);
        size_t nextTail = (currentTail + 1) % capacity;

        if (nextTail == head.load(std::memory_order_acquire)) {
            return false; // Queue is full
        }

        buffer[currentTail] = item;
        tail.store(nextTail, std::memory_order_release);
        return true;
    }

    bool dequeue(T& item) {
        size_t currentHead = head.load(std::memory_order_relaxed);

        if (currentHead == tail.load(std::memory_order_acquire)) {
            return false; // Queue is empty
        }

        item = buffer[currentHead];
        head.store((currentHead + 1) % capacity, std::memory_order_release);
        return true;
    }

    bool is_empty() const {
        return head.load(std::memory_order_acquire) == tail.load(std::memory_order_acquire);
    }

private:
    std::vector<T> buffer;
    std::atomic<size_t> head;
    std::atomic<size_t> tail;
    const size_t capacity;
};

#endif // LOCKFREEQUEUE_H
