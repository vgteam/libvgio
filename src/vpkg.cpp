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

}

}
