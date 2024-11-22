#include "vg/io/stream.hpp"
#include "vg/io/blocked_gzip_output_stream.hpp"

namespace vg {

namespace io {

using namespace std;

void finish(std::ostream& out, bool compressed) {
    if (compressed) {
        // Put an EOF on the stream by making a writer, marking it as EOF, and letting it clean up.
        BlockedGzipOutputStream bgzip_out(out);
        bgzip_out.EndFile();
    }
}

size_t get_stream_length(std::istream& in) {
    in.clear();
    // Get where we are right now
    auto start_pos = in.tellg();
    if (start_pos < 0 || !in.good()) {
        // We can't get a position to seek back to
        return std::numeric_limits<size_t>::max();
    }
    // Go to the end
    in.seekg(0, std::ios_base::end);
    auto end_pos = in.tellg();
    // Go back
    in.seekg(start_pos);
    in.clear();
    if (end_pos < 0) {
        // End position wasn't tellable.
        return std::numeric_limits<size_t>::max();
    }
    return end_pos;
}

size_t get_stream_position(std::istream& in) {
    auto cur_pos = in.tellg();
    if (cur_pos < 0) {
        return std::numeric_limits<size_t>::max();
    }
    return cur_pos;
}

}

}

