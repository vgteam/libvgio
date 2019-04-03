#ifndef VG_IO_BASIC_STREAM_HPP
#define VG_IO_BASIC_STREAM_HPP

#include <string>
#include "vg/vg.pb.h"

using std::string;
using vg::Graph;

namespace vg {
namespace io {

Graph inputStream(const string&);
void outputStream(const Graph&);

}
}

#endif
