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

std::function<void(size_t, size_t)> NO_PROGRESS = [](size_t, size_t) {};
std::function<bool(void)> NO_WAIT = []() { return true; };

}

}

