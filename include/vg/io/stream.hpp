#ifndef VG_IO_STREAM_HPP_INCLUDED
#define VG_IO_STREAM_HPP_INCLUDED

// de/serialization of protobuf objects from/to a length-prefixed, gzipped binary stream
// from http://www.mail-archive.com/protobuf@googlegroups.com/msg03417.html

#include <cassert>
#include <iostream>
#include <istream>
#include <fstream>
#include <functional>
#include <vector>
#include <list>
#include <limits>

#include "registry.hpp"
#include "message_iterator.hpp"
#include "protobuf_iterator.hpp"
#include "protobuf_emitter.hpp"

namespace vg {

namespace io {

using namespace std;

/// Get the length of the input stream, or std::numeric_limits<size_t>::max() if unavailable.
size_t get_stream_length(std::istream& in);
/// Get the current offset in the input stream, or std::numeric_limits<size_t>::max() if unavailable.
size_t get_stream_position(std::istream& in);

/// Write the EOF marker to the given stream, so that readers won't complain that it might be truncated when they read it in.
/// Internal EOF markers MAY exist, but a file SHOULD have exactly one EOF marker at its end.
/// Needs to know if the output stream is compressed or not. Note that uncompressed streams don't actually have nonempty EOF markers.
void finish(std::ostream& out, bool compressed = true);

/// Write objects. count should be equal to the number of objects to write.
/// count is written before the objects, but if it is 0, it is not written. To
/// get the objects, calls lambda with the index of the object to retrieve. If
/// not all objects are written, return false, otherwise true.
/// Needs to know whether to BGZF-compress the output or not.
template <typename T>
bool write(std::ostream& out, size_t count, const std::function<T&(size_t)>& lambda, bool compressed = true) {

    // Wrap stream in an emitter
    ProtobufEmitter<T> emitter(out, compressed);
    
    for (size_t i = 0; i < count; i++) {
        // Write each item.
        emitter.write_copy(lambda(i));
    }
    
    return true;
}

/// Write objects. count should be equal to the number of objects to write.
/// count is written before the objects, but if it is 0, it is not written. To
/// get the objects, calls lambda with the index of the object to retrieve. If
/// not all objects are written, return false, otherwise true.
/// This implementation takes a function that returns actual objects and not references.
/// Needs to know whether to BGZF-compress the output or not.
template <typename T>
bool write(std::ostream& out, size_t count, const std::function<T(size_t)>& lambda, bool compressed = true) {

    static_assert(!std::is_reference<T>::value, "This write() implementation doesn't operate on references");

    // Wrap stream in an emitter
    ProtobufEmitter<T> emitter(out, compressed);
    
    for (size_t i = 0; i < count; i++) {
        // Write each item.
        emitter.write_copy(lambda(i));
    }
    
    return true;
}

/// Start, continue, or finish a buffered stream of objects.
/// If the length of the buffer is greater than the limit, writes the buffer out.
/// Otherwise, leaves the objects in the buffer.
/// Must be called with a buffer limit of 0 after all the objects have been produced, to flush the buffer.
/// When called with a buffer limit of 0, automatically appends an EOF marker.
/// Returns true unless an error occurs.
/// Needs to know whether to BGZF-compress the output or not.
template <typename T>
bool write_buffered(std::ostream& out, std::vector<T>& buffer, size_t buffer_limit, bool compressed = true) {
    bool wrote = false;
    if (buffer.size() >= buffer_limit) {
        std::function<T(size_t)> lambda = [&buffer](size_t n) { return buffer.at(n); };
#pragma omp critical (stream_out)
        wrote = write(out, buffer.size(), lambda, compressed);
        buffer.clear();
    }
    if (buffer_limit == 0) {
        // The session is over. Append the EOF marker.
        finish(out, compressed);
    }
    return wrote;
}

/// Write a single message to a file.
template <typename T>
void write_to_file(const T& item, const string& filename) {
    ofstream out(filename);
    vector<T> items = { item };
    write_buffered(out, items, 1, false);
    out.close();
}

// Default progress function that does nothing.
const std::function<void(size_t, size_t)> NO_PROGRESS = [](size_t, size_t) {};
// Default waiting function that always returns true.
const std::function<bool(void)> NO_WAIT = []() { return true; };

template <typename T>
void for_each(std::istream& in,
              const std::function<void(int64_t, T&)>& lambda,
              const std::function<void(size_t, size_t)>& progress = NO_PROGRESS) {
    
    size_t stream_length = get_stream_length(in);
    if (stream_length == std::numeric_limits<size_t>::max()) {
        // Tell the progress function there will be no progress.
        progress(stream_length, stream_length);
    }

    for(ProtobufIterator<T> it(in); it.has_current(); ++it) {
        // For each message in the file, parse and process it with its group VO (or -1)
        lambda(it.tell_group(), *it);

        if (stream_length != std::numeric_limits<size_t>::max()) {
            // Do progress.
            // We know ProtobufIterator uses single-threaded decompression, so
            // we can act on the stream directly.
            progress(get_stream_position(in), stream_length);
        }
    }
}

template <typename T>
void for_each(std::istream& in,
              const std::function<void(T&)>& lambda,
              const std::function<void(size_t, size_t)>& progress = NO_PROGRESS) {
    for_each(in, static_cast<const typename std::function<void(int64_t, T&)>&>([&lambda](int64_t virtual_offset, T& item) {
        lambda(item);
    }), progress);
}

// Parallelized versions of for_each

// First, an internal implementation underlying several variants below.
// lambda2 is invoked on interleaved pairs of elements from the stream. The
// elements of each pair are in order, but the overall order in which lambda2
// is invoked on pairs is undefined (concurrent). lambda1 is invoked on an odd
// last element of the stream, if any.
// objects will be handed off to worker threads in batches of "batch_size" which
// must be divisible by 2.
// The progress function is invoked periodically with the input stream offset
// and length, or std::numeric_limits<size_t>::max() if they are unavailable.

template <typename T>
void for_each_parallel_impl(std::istream& in,
                            const std::function<void(T&,T&)>& lambda2,
                            const std::function<void(T&)>& lambda1,
                            const std::function<bool(void)>& single_threaded_until_true,
                            size_t batch_size,
                            const std::function<void(size_t, size_t)>& progress = NO_PROGRESS) {

    size_t stream_length = get_stream_length(in);
    if (stream_length == std::numeric_limits<size_t>::max()) {
        // Tell the progress function there will be no progress.
        progress(stream_length, stream_length);
    }

    assert(batch_size % 2 == 0); //for_each_parallel::batch_size must be even
    // max # of such batches to be holding in memory
    size_t max_batches_outstanding = 256;
    // max # we will ever increase the batch buffer to
    const size_t max_max_batches_outstanding = 1 << 13; // 8192
    // number of batches currently being processed
    size_t batches_outstanding = 0;
    
#ifdef debug
    cerr << "Looping over file in batches of size " << batch_size << endl;
#endif

    // Construct the MessageIterator in the main thread so exceptions in its
    // startup can be caught.

    // We do our own multi-threaded Protobuf decoding, but we batch up our
    // strings by pulling them from this iterator, which we also
    // multi-thread for decompression.
    //
    // Note that as long as this exists, we **may not** use the "in"
    // stream! Even just to tell() it for the current position! This will
    // start backgorund threads that use the stream, and on mac at least
    // even a tellg() can mutate the stream internally and cause the
    // background threads to segfault.
    MessageIterator message_it(in, false, 8);

    // this loop handles a chunked file with many pieces
    // such as we might write in a multithreaded process
    #pragma omp parallel default(none) shared(message_it, lambda1, lambda2, progress, stream_length, batches_outstanding, max_batches_outstanding, single_threaded_until_true, cerr, batch_size)
    #pragma omp single
    {
        auto handle = [](bool retval) -> void {
            if (!retval) throw std::runtime_error("obsolete, invalid, or corrupt protobuf input");
        };

        std::vector<std::string> *batch = nullptr;
        
        bool first_message = true;

        while (message_it.has_current()) {
            // Until we run out of messages, grab them with their tags
            auto tag_and_data = std::move(message_it.take());
            
            // Check the tag.
            // TODO: we should only do this when it changes!
            bool right_tag = Registry::check_protobuf_tag<T>(tag_and_data.first);
            if (!right_tag) {
                // This isn't the data we were expecting.
                if (first_message) {
                    // If this happens on the very first message, we know this is the wrong kind of stream.
                    throw std::runtime_error("expected a stream of " + T::descriptor()->full_name() + " but found first message with tag " + tag_and_data.first);
                } else {
                    // On other mesages, just skip them if they aren't what we care about.
                    first_message = false;
                    continue;
                }
            }
            first_message = false;
            
            // If the tag checks out
            
            // Make sure we have a batch
            if (batch == nullptr) {
                batch = new vector<string>();
            }
            
            if (tag_and_data.second.get() != nullptr) {
                // Add the message to the batch, if it exists
                batch->push_back(std::move(*tag_and_data.second));
            }
            
            if (batch->size() == batch_size) {
#ifdef debug
                cerr << "Found full batch of size " << batch_size << endl;
#endif
            
                // time to enqueue this batch for processing. first, block if
                // we've hit max_batches_outstanding.
                size_t b;
#pragma omp atomic capture
                b = ++batches_outstanding;
                
                bool do_single_threaded = !single_threaded_until_true();
                if (b >= max_batches_outstanding || do_single_threaded) {
                    
#ifdef debug
                    cerr << "Run batch in current thread" << endl;
#endif
                    
                    // process this batch in the current thread
                    {
                        T obj1, obj2;
                        for (int i = 0; i<batch_size; i+=2) {
                            // parse protobuf objects and invoke lambda on the pair
                            handle(ProtobufIterator<T>::parse_from_string(obj1, batch->at(i)));
                            handle(ProtobufIterator<T>::parse_from_string(obj2, batch->at(i+1)));
                            lambda2(obj1,obj2);
                        }
                    } // scope obj1 & obj2
                    delete batch;
#pragma omp atomic capture
                    b = --batches_outstanding;
                    
                    if (4 * b / 3 < max_batches_outstanding
                        && max_batches_outstanding < max_max_batches_outstanding
                        && !do_single_threaded) {
                        // we went through at least 1/4 of the batch buffer while we were doing this thread's batch
                        // this looks risky, since we want the batch buffer to stay populated the entire time we're
                        // occupying this thread on compute, so let's increase the batch buffer size
                        // (skip this adjustment if you're in single-threaded mode and thus expect the buffer to be
                        // empty)
                        max_batches_outstanding *= 2;
                    }
                }
                else {
#ifdef debug
                    cerr << "Run batch in task" << endl;
#endif
                
                    // spawn a task in another thread to process this batch
#pragma omp task default(none) firstprivate(batch) shared(batches_outstanding, lambda2, handle, single_threaded_until_true, cerr, batch_size)
                    {
#ifdef debug
                        cerr << "Batch task is running" << endl;
#endif
                        
                        {
                            T obj1, obj2;
                            for (int i = 0; i<batch_size; i+=2) {
                                // parse protobuf objects and invoke lambda on the pair
                                handle(ProtobufIterator<T>::parse_from_string(obj1, batch->at(i)));
                                handle(ProtobufIterator<T>::parse_from_string(obj2, batch->at(i+1)));
                                lambda2(obj1,obj2);
                            }
                        } // scope obj1 & obj2
                        delete batch;
#pragma omp atomic update
                        batches_outstanding--;
                    }
                }

                batch = nullptr;
            }

            if (stream_length != std::numeric_limits<size_t>::max()) {
                // Do progress. But we can't use get_stream_position because we
                // can't use the stream!
                //
                // We also can't get at htslib's bgzf_htell, which synchronizes
                // with the real read threads but isn't exposed as a symbol.
                //
                // So we get the virtual offset and shift off the
                // non-block-address bits so it is a real backing file offset,
                // just with BGZF block resolution.
                progress(message_it.tell_group()>>16, stream_length);
            }
        }

        #pragma omp taskwait
        // process final batch
        if (batch) {
#ifdef debug
            cerr << "Run final batch of size " << batch->size() << " in current thread" << endl;
#endif
            if (!batch->empty()) {
                // We require the batch to not be empty (so we can subtract from the size).
                T obj1, obj2;
                int i = 0;
                for (; i < batch->size()-1; i+=2) {
                    handle(ProtobufIterator<T>::parse_from_string(obj1, batch->at(i)));
                    handle(ProtobufIterator<T>::parse_from_string(obj2, batch->at(i+1)));
                    lambda2(obj1, obj2);
                }
                if (i == batch->size()-1) { // odd last object
                    handle(ProtobufIterator<T>::parse_from_string(obj1, batch->at(i)));
                    lambda1(obj1);
                }
            }
            delete batch;
        }
    }
}

// parallel iteration over interleaved pairs of elements; error out if there's an odd number of elements
template <typename T>
void for_each_interleaved_pair_parallel(std::istream& in,
                                        const std::function<void(T&,T&)>& lambda2,
                                        size_t batch_size = 256,
                                        const std::function<void(size_t, size_t)>& progress = NO_PROGRESS) {
    std::function<void(T&)> err1 = [](T&){
        throw std::runtime_error("io::for_each_interleaved_pair_parallel: expected input stream of interleaved pairs, but it had odd number of elements");
    };
    for_each_parallel_impl(in, lambda2, err1, NO_WAIT, batch_size, progress);
}
    
template <typename T>
void for_each_interleaved_pair_parallel_after_wait(std::istream& in,
                                                   const std::function<void(T&,T&)>& lambda2,
                                                   const std::function<bool(void)>& single_threaded_until_true,
                                                   size_t batch_size = 256,
                                                   const std::function<void(size_t, size_t)>& progress = NO_PROGRESS) {
    std::function<void(T&)> err1 = [](T&){
        throw std::runtime_error("io::for_each_interleaved_pair_parallel: expected input stream of interleaved pairs, but it had odd number of elements");
    };
    for_each_parallel_impl(in, lambda2, err1, single_threaded_until_true, batch_size, progress);
}

// parallelized for each individual element
template <typename T>
void for_each_parallel(std::istream& in,
                       const std::function<void(T&)>& lambda1,
                       size_t batch_size = 256,
                       const std::function<void(size_t, size_t)>& progress = NO_PROGRESS) {
    std::function<void(T&,T&)> lambda2 = [&lambda1](T& o1, T& o2) { lambda1(o1); lambda1(o2); };
    for_each_parallel_impl(in, lambda2, lambda1, NO_WAIT, batch_size, progress);
}

}

}

#endif
