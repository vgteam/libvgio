/**
 * \file vpkg.cpp: Implementations for VPKG loader/saver functions.
 */


#include "vg/io/vpkg.hpp"

namespace vg {

namespace io {

using namespace std;

void VPKG::with_save_stream(ostream& to, const string& tag, const function<void(ostream&)>& use_stream) {
    // TODO: This is sort of redundant with the bare saver wrapping logic. Unify them?

    // Make a MessageEmitter
    MessageEmitter emitter(to);
    
    with_function_calling_stream([&emitter, &tag](const string& data) {
        // Save each chunk of stream data to the emitter with the right tag
        emitter.write_copy(tag, data);
    }, [&use_stream](ostream& tagged) {
        // Run the stream-using function on our stream that goes into the emitter
        use_stream(tagged);
    });
}

bool VPKG::sniff_magic(istream& stream, const string& magic) {

    if (!stream) {
        // Can't read anything, so obviously it can't match.
        return false;
    }
    
    // Work out how many characters to try and sniff.
    // We require that our C++ STL can do this much ungetting, even though the
    // standard guarantees absolutely no ungetting.
    size_t to_sniff = magic.size();
    
    // Allocate a buffer
    char buffer[to_sniff];
    // Have a cursor in the buffer
    size_t buffer_used = 0;
    
    while (stream.peek() != EOF && buffer_used < to_sniff) {
        // Until we fill the buffer or would hit EOF, fill the buffer
        buffer[buffer_used] = (char) stream.get();
        buffer_used++;
    }

    for (size_t i = 0; i < buffer_used; i++) {
        // Now unget all the characters again.
        // C++11 says we can unget from EOF.
        stream.unget();
        if (!stream) {
            // We did something the stream disliked.
            throw runtime_error("Ungetting failed after " + to_string(i) + " characters");
        }
    }
    
    // Now all the characters are back in the stream.
    
    if (!stream) {
        // We reached EOF when sniffing the magic. We managed to unget
        // everything (maybe the file is empty). But we need to clear errors on
        // the stream so it is like it was when we started.
        stream.clear();
    }

    if (buffer_used < magic.size()) {
        // We ran out of data
        return false;
    }
    
    for (size_t i = 0; i < buffer_used; i++) {
        // Check each character
        if (buffer[i] != magic[i]) {
            // And on a mismatch, fail
            return false;
        }
    }
    
    // If we get here, there were no mismatches
    return true;
}

}

}
