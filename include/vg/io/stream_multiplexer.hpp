#ifndef VG_IO_STREAM_MULTIPLEXER_HPP_INCLUDED
#define VG_IO_STREAM_MULTIPLEXER_HPP_INCLUDED

/**
 * \file stream_multiplexer.chpp
 * Defines a multiplexer for combining output from multiple threads.
 */


#include <sstream>
#include <mutex>
#include <thread>
#include <atomic>
#include <vector>
#include <list>

namespace vg {

namespace io {

using namespace std;


/**
 * Tool to allow multiple threads to write to streams that are multiplexed into
 * an output stream, by breaking at allowed points.
 *
 * Assumes an external source of thread numbering.
 */
class StreamMultiplexer {

public:
    /**
     * Make a new StreamMultiplexer sending output to the given output stream.
     *
     * Needs to know the maximum number of threads that will use the multiplexer.
     */
    StreamMultiplexer(ostream& backing, size_t max_threads);
    
    /**
     * Clean up and flush a StreamMultiplexer.
     *
     * Assumes a final breakpoint on all streams.
     */
    ~StreamMultiplexer();
    
    // Do not allow the StreamMultiplexer itself to be copied or moved.
    // This is because it has to own streams.
    StreamMultiplexer(const StreamMultiplexer& other) = delete;
    StreamMultiplexer(StreamMultiplexer&& other) = delete;
    StreamMultiplexer& operator=(const StreamMultiplexer& other) = delete;
    StreamMultiplexer& operator=(StreamMultiplexer&& other) = delete;
    
    
    /**
     * Get the stream for the thread with the given number to write to.
     * This stream will never change address for a given thread, but the object
     * may be destroyed or recreated in place, or moved out of, during calls to
     * register_breakpoint().
     *
     * Note that using this function in an "untied" (i.e. not thread bound) OMP
     * task is not supported!
     */
    ostream& get_thread_stream(size_t thread_number);

    /**
     * This function must be called after batches of writes to the stream from
     * get_thread_stream(), at points where it is legal, given the output
     * format being constructed, to switch in the final output stream from one
     * thread's output to another's.
     *
     * It may adjust or replace the object that get_thread_stream() returned,
     * but it must leave a stream at the same address.
     *
     * Takes the thread number of the thread whose output we can break.
     */
    void register_breakpoint(size_t thread_number);
    
    /**
     * Check if the multiplexer would like a breakpoint (i.e. has a substantial
     * amount of data been written since the last breakpoint.
     *
     * Writers can check this and conditionally flush their internal buffers to
     * get to a good interleave-able state at regular intervals.
     */
    bool want_breakpoint(size_t thread_number);
    
    /**
     * Send along whatever has been written for the given thread's stream to
     * the output stream. Only returns once any subsequent write by another
     * thread is guaranteed to appear later in the file than what has been
     * written by the given thread so far. Implicitly also creates a
     * breakpoint.
     */
     void register_barrier(size_t thread_number);
     
    /**
     * Cancel the writing of and discard all data written since the previous
     * breakpoint for the given thread.
     */
    void discard_to_breakpoint(size_t thread_number);
    
    /**
     * Cancel the writing of the last count bytes written since the last
     * breakpoint for the given thread. If count is more than the number of
     * bytes since the last breakpoint, rewinds only to the last breakpoint.
     */
     void discard_bytes(size_t thread_number, size_t count);
    
private:

    /// Remember the backing stream we wrap
    ostream& backing_stream;

    /// Each thread gets a slot in this vector for a stringstream it is
    /// supposed to be currently writing to.
    vector<stringstream> thread_streams;
    
    /// Not every breakpoint results in the stream for a thread being cleared
    /// out and enqueued. We only actually use a breakpoint if we have enough
    /// data in the stream. But we still have to support discard_to_breakpoint.
    /// So we keep a cursor for where the most recent breakpoint was. The
    /// actual current position in each stream is tracked by the put pointer
    /// (seekp()/tellp()).
    vector<size_t> thread_breakpoint_cursors;
    
    /// When a thread reaches a breakpoint and its stringstream is big enough,
    /// its full stringstream is moved into this queue at the back, and a new
    /// one created in place.
    vector<list<stringstream>> thread_queues;
    /// This tracks the number of bytes of data in each thread's queue.
    vector<size_t> thread_queue_byte_counts;
    /// Access to each thread's queue and byte count is controlled by a mutex.
    /// The mutex only has to be held long enough to a little moving, and can
    /// only ever be contended between two threads.
    vector<mutex> thread_queue_mutexes;
    
    /// When set to true, cause the writer thread to finish writing all queues and terminate.
    atomic<bool> writer_stop;
    
    /// This thread is responsible for servicing all the queues and dumping the
    /// bytes to the real backing stream.
    thread writer_thread;
    
    /// If a thread's queue has this many bytes or more in it, block inside
    /// register_breakpoint() until it gets below this threshold.
    static size_t MAX_THREAD_QUEUE_BYTES;
    /// To prevent constant locking and unlocking of the queues, we want each
    /// item to be relatively substantial.
    static size_t MIN_QUEUE_ITEM_BYTES;
   
    /**
     * Function which is run as the writer thread.
     * Empties queues as fast as it can.
     */
    void writer_thread_function();
    
};

}

}

#endif
