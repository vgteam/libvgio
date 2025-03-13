/**
 * \file stream_multiplexer.cpp
 * Implementations for the StreamMultiplexer for merging output from threads.
 */

#include "vg/io/stream_multiplexer.hpp"
#include <iostream>

namespace vg {

namespace io {

using namespace std;

/// Don't deal with anything smaller than a few BGZF blocks.
const size_t StreamMultiplexer::MIN_QUEUE_ITEM_BYTES = 10 * 64 * 1024;

/// Don't allow more than a few items per ring buffer
const size_t StreamMultiplexer::RING_BUFFER_SIZE = 10;

StreamMultiplexer::StreamMultiplexer(ostream& backing, size_t max_threads) :
    backing_stream(backing),
    thread_streams(max_threads),
    thread_breakpoint_cursors(max_threads, 0),
    thread_queues(max_threads, vector<string>(RING_BUFFER_SIZE)),
    thread_queue_empty_slots(max_threads, 0),
    thread_queue_filled_slots(max_threads, 0),
    thread_queue_byte_counts(max_threads, 0), 
    thread_queue_mutexes(max_threads),
    writer_stop(false),
    writer_thread(&StreamMultiplexer::writer_thread_function, this) {
    // Nothing to do! Writer thread is now running!
}

StreamMultiplexer::~StreamMultiplexer() {
#ifdef debug
    cerr << "StreamMultiplexer destructing" << endl;
#endif

    // Tell the writer to finish.
    writer_stop.store(true);
    // Wait for it to finish.
    writer_thread.join();
    
    // Make sure to flush the backing stream, so output is on disk.
    // Probably not necessary, but makes sense.
    backing_stream.flush();

    // Do one last check to make sure the data was taken.
    if (backing_stream.fail()) {
        // We're not allowed to throw here or the compiler will warn, because we're in a destructor.
        // So just exit.
        // TODO: What if the library user thinks they can somehow recover from a
        // write error? We'd have to change this ans also stop throwing exceptions
        // in the writer thread.
        std::cerr << "error:[vg::io::StreamMultiplexer]: Could not write output." << std::endl;
        exit(1);
    }
    
#ifdef debug
    cerr << "StreamMultiplexer destroyed" << endl;
#endif
}

ostream& StreamMultiplexer::get_thread_stream(size_t thread_number) {
    // The stream is always in the same place for a given thread.
    return thread_streams.at(thread_number);
}

void StreamMultiplexer::register_breakpoint(size_t thread_number) {
    // The thread says we can break here.
    // Also, we are in the thread.
    
    // Get our stream
    stringstream& our_stream = thread_streams.at(thread_number);
    
    // See how much data it has
    size_t item_bytes = our_stream.tellp();
    
    if (item_bytes >= MIN_QUEUE_ITEM_BYTES) {
        // We have enough data to justify a block.
        
#ifdef debug
        cerr << "StreamMultiplexer registered breakpoint for " << item_bytes << " bytes in thread " << thread_number << endl;
#endif
        
        // Lock our queue
        thread_queue_mutexes[thread_number].lock();
        
        while (ring_buffer_full(thread_number)) {
            // The queue is over-full.
            
            // Unlock, yield, and lock again, to give the writer time to empty our queue.
            thread_queue_mutexes[thread_number].unlock();
            std::this_thread::yield();
            thread_queue_mutexes[thread_number].lock();
            // TODO: use a condition variable here to alert the writer that we want writing.
        }
        
        // Add in the space usage
        thread_queue_byte_counts[thread_number] += item_bytes;
        
        // Move the right number of bytes from the stringstream into the queue at the back
        // TODO: can we avoid a copy here?
        ring_buffer_push(thread_number) = std::move(our_stream.str().substr(0, item_bytes));
        
        // Unlock the queue
        thread_queue_mutexes[thread_number].unlock();
        
        // Reset the stream so it can be filled up again.
        // Empty the contents and clear the state bits.
        // Move might do this but it's not guaranteed.
        our_stream.str(std::string());
        our_stream.clear();
        
        // Reset the breakpoint cursor
        thread_breakpoint_cursors[thread_number] = 0;
    } else {
#ifdef debug
        cerr << "StreamMultiplexer skipped breakpoint for " << item_bytes << " bytes in thread " << thread_number << endl;
#endif

        // We aren't going to actually cut the data here, but remember the
        // breakpoint position in case we have to rewind to it.
        thread_breakpoint_cursors[thread_number] = item_bytes;
    }
    
    
}

bool StreamMultiplexer::want_breakpoint(size_t thread_number) {
    stringstream& our_stream = thread_streams.at(thread_number);
    
    // See how much data it has
    size_t item_bytes = our_stream.tellp();
    
#ifdef debug
    cerr << "Checking for breakpoint at " << item_bytes << "/" << MIN_QUEUE_ITEM_BYTES << " bytes" << endl;
#endif
    
    // Ask them to give us a break if they have enough bytes for us to ship out.
    return (item_bytes >= MIN_QUEUE_ITEM_BYTES);
}

void StreamMultiplexer::register_barrier(size_t thread_number) {
    // Get our stream
    stringstream& our_stream = thread_streams.at(thread_number);
    
    // See how much data it has
    size_t item_bytes = our_stream.tellp();
    
    // Whether our block is big enough or not, put it in the queue
    
    // Lock our queue
    thread_queue_mutexes[thread_number].lock();
    
    while (ring_buffer_full(thread_number)) {
        // The queue is over-full.
        
        // Unlock, yield, and lock again, to give the writer time to empty our queue.
        thread_queue_mutexes[thread_number].unlock();
        std::this_thread::yield();
        thread_queue_mutexes[thread_number].lock();
        // TODO: use a condition variable here to alert the writer that we want writing.
    }
    
    // Add in the space usage
    thread_queue_byte_counts[thread_number] += item_bytes;
    
    // Move the right number of bytes from the stringstream into the queue at the back
    // TODO: can we avoid a copy here?
    ring_buffer_push(thread_number) = std::move(our_stream.str().substr(0, item_bytes));
    
    // Unlock the queue
    thread_queue_mutexes[thread_number].unlock();
    
    // Reset the stream so it can be filled up again.
    // Empty the contents and clear the state bits.
    // Move might do this but it's not guaranteed.
    our_stream.str(std::string());
    our_stream.clear();
    
    // Reset the breakpoint cursor
    thread_breakpoint_cursors[thread_number] = 0;
    
    // Don't return until our queue is empty. If our queue is empty, our data
    // will come out before anything written subsequently, since the writer
    // thread either has already written our data or is currently doing it.
    
    // Yield at least once to give the writer a chance to write the first time around.
    std::this_thread::yield();
    
    thread_queue_mutexes[thread_number].lock();
    while (!ring_buffer_empty(thread_number)) {
        // Unlock, yield, and lock again, to give the writer time to empty our queue.
        thread_queue_mutexes[thread_number].unlock();
        std::this_thread::yield();
        thread_queue_mutexes[thread_number].lock();
    }
    thread_queue_mutexes[thread_number].unlock();
}

void StreamMultiplexer::discard_to_breakpoint(size_t thread_number) {
    // Get our stream
    stringstream& our_stream = thread_streams.at(thread_number);
    
    // Get the write position in the stream
    size_t item_bytes = our_stream.tellp();
    
    if (item_bytes > thread_breakpoint_cursors[thread_number]) {
        // We have advanced past the previous breakpoint and need to rewind to it.
        our_stream.seekp(thread_breakpoint_cursors[thread_number]);
        // Anything after the put pointer will be ignored when outputting the stream's contents
    }
}

void StreamMultiplexer::discard_bytes(size_t thread_number, size_t count) {
    // Get our stream
    stringstream& our_stream = thread_streams.at(thread_number);
    
    // Get the write position in the stream
    size_t item_bytes = our_stream.tellp();
    
    if (count > item_bytes) {
        // We want to rewind past the very beginning. Clamp.
        count = item_bytes;
    }
    
    // Work out how many bytes would be left
    size_t new_item_bytes = item_bytes - count;
    
    // Clamp to the last breakpoint
    new_item_bytes = max(thread_breakpoint_cursors[thread_number], new_item_bytes);
    
    // Seek to the new position.
    our_stream.seekp(new_item_bytes);
}

void StreamMultiplexer::writer_thread_function() {

#ifdef debug
    cerr << "StreamMultiplexer writer starting" << endl;
#endif

#ifdef debug
    // Track the max bytes obeserved in any queue
    size_t high_water_bytes = 0;
#endif

    while(!writer_stop.load()) {
        // We have not been asked to stop.
        
        // We set this if we write anything.
        // If we don't have any work to do on a whole pass, we yield so we
        // don't constantly spin.
        bool found_data = false;
        
        for (size_t i = 0; i < thread_queues.size(); i++) {
            // For each queue
            
            // Lock it
            thread_queue_mutexes[i].lock();
            if (!ring_buffer_empty(i)) {
#ifdef debug
                // Record data size
                high_water_bytes = max(high_water_bytes, thread_queue_byte_counts[i]);
#endif
                
                // Find the chunk to write.
                const string& emptying = ring_buffer_peek(i);
                
                // Count how many bytes are in the string.
                size_t data_bytes = emptying.size();
                
#ifdef debug
                cerr << "StreamMultiplexer writing " << data_bytes  << " bytes from thread " << i << endl;
#endif
                
                // Record we removed its data from the queue
                thread_queue_byte_counts[i] -= data_bytes;
                
                // Unlock now
                thread_queue_mutexes[i].unlock();
                
                // Dump the data block. We know it won't leave the queue unless we pop it.
                backing_stream << emptying;
                if (backing_stream.fail()) {
                    // Fail early if the disk is not taking our data.
                    // No point in throwing an exception here since we're in a
                    // writer thread and the library user wouldn't be able to
                    // catch it. 
                    std::cerr << "error:[vg::io::StreamMultiplexer]: Could not write output." << std::endl;
                    exit(1);
                }
                
                /// Lock again and pop. Nobody else could have removed the thing we were working on.
                thread_queue_mutexes[i].lock();
                ring_buffer_pop(i);
                thread_queue_mutexes[i].unlock();
                
                // Say we had work to do
                found_data = true;
            } else {
                // Unlock right away
                thread_queue_mutexes[i].unlock();
            }
        }
        
        if (!found_data) {
            // Don't spin constantly with nothing to do.
            std::this_thread::yield();
        }
    }
    
    // Now we have been asked to stop. Take care of saving everything in the
    // queues, and the final buffers if they were too small.
    // No locks since none of the other threads are allowed to be writing now
    // (our destructor has started).
    for (size_t i = 0; i < thread_queues.size(); i++) {
#ifdef debug
        // Record sizes
        high_water_bytes = max(high_water_bytes, thread_queue_byte_counts[i]);
#endif
        while (!ring_buffer_empty(i)) {
            auto& item = ring_buffer_peek(i);
            
            // Get how many bytes are in the string buffer.
            size_t data_bytes = item.size();
            
#ifdef debug
            cerr << "StreamMultiplexer finishing with " << data_bytes << " queued bytes from thread " << i << endl;
#endif
            
            backing_stream << item;
            if (backing_stream.fail()) {
                std::cerr << "error:[vg::io::StreamMultiplexer]: Could not write output." << std::endl;
                exit(1);
            }
            
            ring_buffer_pop(i);
        }
    }
    for (auto& item : thread_streams) {
        // Ship out the final partial chunks without sending them through the queues.
        
        // Get how many bytes are not rewound.
        size_t data_bytes = item.tellp();
        
        if (data_bytes > 0) {
#ifdef debug
            cerr << "StreamMultiplexer finishing with " << data_bytes << " unqueued bytes" << endl;
#endif
        
            backing_stream << item.str().substr(0, data_bytes);
            if (backing_stream.fail()) {
                std::cerr << "error:[vg::io::StreamMultiplexer]: Could not write output." << std::endl;
                exit(1);
            }
        }
    }
    
#ifdef debug
    cerr << "StreamMultiplexer high water mark: " << high_water_bytes << " bytes across " << thread_queues.size() << " threads" << endl;
#endif
}

bool StreamMultiplexer::ring_buffer_full(size_t thread_number) const {
    auto& empty = thread_queue_empty_slots[thread_number];
    auto& filled = thread_queue_filled_slots[thread_number];

    // We are full if empty is 1 before filled, because we leave one slot open
    return (empty + 1 == filled || (empty + 1 == RING_BUFFER_SIZE && filled == 0));
}

bool StreamMultiplexer::ring_buffer_empty(size_t thread_number) const {
    auto& empty = thread_queue_empty_slots[thread_number];
    auto& filled = thread_queue_filled_slots[thread_number];

    // We are empty if empty is filled
    return (empty == filled);
}

string& StreamMultiplexer::ring_buffer_push(size_t thread_number) {
    auto& empty = thread_queue_empty_slots[thread_number];
    auto& buffer = thread_queues[thread_number];
    
    // Grab the empty slot
    auto& slot = buffer[empty];
    
    // Advance the empty cursor
    empty++;
    if (empty == RING_BUFFER_SIZE) {
        // And wrap
        empty = 0;
    }
    
    return slot;
}

const string& StreamMultiplexer::ring_buffer_peek(size_t thread_number) {
    auto& filled = thread_queue_filled_slots[thread_number];
    auto& buffer = thread_queues[thread_number];
    
    return buffer[filled];
}

void StreamMultiplexer::ring_buffer_pop(size_t thread_number) {
    auto& filled = thread_queue_filled_slots[thread_number];
    
    // Advance the filled cursor
    filled++;
    if (filled == RING_BUFFER_SIZE) {
        // And wrap
        filled = 0;
    }
}


}

}



