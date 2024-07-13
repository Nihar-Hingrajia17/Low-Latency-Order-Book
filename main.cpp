#include <sched.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include "FeedHandler.h"
#include "FHErrorTracker.h"
#include "HRTimer.h"
#include "LagHistogram.h"
#include "Utilities.h"
#include "LockFreeQueue.h"

using namespace OrderBook;

// Define queue capacity
const size_t QUEUE_CAPACITY = 1024;

// Create a lock-free queue for messages
LockFreeQueue<std::string> messageQueue(QUEUE_CAPACITY);

// Atomic flag to signal when reading is done
std::atomic<bool> done(false);

void readMessages(const std::string& filename) {
    FILE *pFile = fopen(filename.c_str(), "r");
    if (pFile == NULL) {
        std::cerr << "Error opening file: " << filename << ". Please check the file and try again." << std::endl;
        done.store(true); // Ensure done is set to true if there is an error
        return;
    }

    size_t len = 0;
    char *buffer = NULL;
    while (!feof(pFile)) {
        ssize_t read = getline(&buffer, &len, pFile);
        if (read == -1) break;
        std::string message(buffer);
        free(buffer); // Free buffer allocated by getline
        buffer = NULL;
        while (!messageQueue.enqueue(message)) {
            // Wait if the queue is full
        }
    }
    fclose(pFile);
    done.store(true); // Signal that reading is done atomically
}

void processMessages(FeedHandler<uint32_t, OrderLevelEntry>& feed) {
    uint32_t counter = 0;
    std::string message;
    while (!done.load() || !messageQueue.is_empty()) {
        if (messageQueue.dequeue(message)) {
            feed.processMessage(&message[0]);
            ++counter;
            if (counter % 10 == 0) {
                feed.printCurrentOrderBook();
            }
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 2) {
        std::cerr << "Usage: OrderBookProcessor <filename>" << std::endl;
        return -1;
    }

    FHErrorTracker::instance()->init();
    FeedHandler<uint32_t, OrderLevelEntry> feed;
    const std::string filename(argv[1]);

    std::thread readerThread(readMessages, filename);
    std::thread processorThread(processMessages, std::ref(feed));

    readerThread.join();
    processorThread.join();

    FHErrorTracker::instance()->printStatistics();
    return 0;
}
