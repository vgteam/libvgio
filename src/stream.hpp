#ifndef VG_IO_STREAM_HPP
#define VG_IO_STREAM_HPP

// std
#include <functional>  // function
#include <iostream>  // cout, istream, ostream
#include <list>
#include <string>
// protobuf
#include "google/protobuf/stubs/common.h"
#include "google/protobuf/io/zero_copy_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/io/gzip_stream.h"
#include "google/protobuf/io/coded_stream.h"

using std::function;
using std::istream;
using std::list;
using std::ostream;
using std::string;
using std::vector;

using namespace google;

namespace vg {
namespace io {

template <typename T> bool
write(ostream& out, uint64_t count, function<T(uint64_t)>& lambda)
{
  protobuf::io::ZeroCopyOutputStream *raw_out =
    new protobuf::io::OstreamOutputStream(&out);
  protobuf::io::GzipOutputStream *gzip_out =
    new protobuf::io::GzipOutputStream(raw_out);
  protobuf::io::CodedOutputStream *coded_out =
    new protobuf::io::CodedOutputStream(gzip_out);

  // prefix the chunk with the number of objects
  coded_out->WriteVarint64(count);

  string s;
  uint64_t written = 0;
  for (uint64_t n = 0; n < count; ++n, ++written) {
    lambda(n).SerializeToString(&s);
    // and prefix each object with its size
    coded_out->WriteVarint32(s.size());
    coded_out->WriteRaw(s.data(), s.size());
  }

  delete coded_out;
  delete gzip_out;
  delete raw_out;

  return !count || written == count;
}

template <typename T> bool
write_buffered(ostream& out, vector<T>& buffer, uint64_t buffer_limit)
{
  bool wrote = false;
  if (buffer.size() >= buffer_limit) {
    function<T(uint64_t)> lambda = [&buffer](uint64_t n) { return buffer.at(n); };
#pragma omp critical (stream_out)
    wrote = write(out, buffer.size(), lambda);
    buffer.clear();
  }
  return wrote;
}

template <typename T>
bool write_buffered_ptr(ostream& out, vector<T*>& buffer, uint64_t buffer_limit)
{
  bool wrote = false;
  if (buffer.size() >= buffer_limit) {
    function<T(uint64_t)> lambda = [&buffer](uint64_t n) { return *buffer.at(n); };
#pragma omp critical (stream_out)
    wrote = write(out, buffer.size(), lambda);
    buffer.clear();
  }
  return wrote;
}

// deserialize the input stream into the objects
// count containts the count read
// takes a callback function to be called on the objects
template <typename T> bool for_each(
  istream& in,
  function<void(T&)>& lambda,
  function<void(uint64_t)>& handle_count)
{
  protobuf::io::ZeroCopyInputStream *raw_in =
    new protobuf::io::IstreamInputStream(&in);
  protobuf::io::GzipInputStream *gzip_in =
    new protobuf::io::GzipInputStream(raw_in);
  protobuf::io::CodedInputStream *coded_in =
    new protobuf::io::CodedInputStream(gzip_in);

  uint64_t count;
  coded_in->ReadVarint64((protobuf::uint64*) &count);
  // this loop handles a chunked file with many pieces
  // such as we might write in a multithreaded process
  if (!count) return !count;
  do {
    handle_count(count);
    string s;
    for (uint64_t i = 0; i < count; ++i) {
      uint32_t msgSize = 0;
      delete coded_in;
      coded_in = new protobuf::io::CodedInputStream(gzip_in);
      // the messages are prefixed by their size
      coded_in->ReadVarint32(&msgSize);
      if ((msgSize > 0) &&
        (coded_in->ReadString(&s, msgSize))) {
        T object;
        object.ParseFromString(s);
        lambda(object);
      }
    }
  } while (coded_in->ReadVarint64((protobuf::uint64*) &count));

  delete coded_in;
  delete gzip_in;
  delete raw_in;

  return !count;
}

template <typename T> bool
for_each(istream& in, function<void(T&)>& lambda)
{
  function<void(uint64_t)> noop = [](uint64_t) { };
  return for_each(in, lambda, noop);
}

template <typename T> bool
for_each_parallel(
  istream& in,
  function<void(T&)>& lambda,
  function<void(uint64_t)>& handle_count)
{
  protobuf::io::ZeroCopyInputStream *raw_in =
    new protobuf::io::IstreamInputStream(&in);
  protobuf::io::GzipInputStream *gzip_in =
    new protobuf::io::GzipInputStream(raw_in);
  protobuf::io::CodedInputStream *coded_in =
    new protobuf::io::CodedInputStream(gzip_in);

  uint64_t count;
  bool more_input = coded_in->ReadVarint64((protobuf::uint64*) &count);
  bool more_objects = false;
  // this loop handles a chunked file with many pieces
  // such as we might write in a multithreaded process
  list<T> objects;
  int64_t object_count = 0;
  int64_t read_threshold = 5000;
  if (!count) {
    return !count;
  }
#pragma omp parallel shared(more_input, more_objects, objects, count, in, lambda, handle_count, raw_in, gzip_in, coded_in)
  do {
    bool has_object = false;
    T object;
#pragma omp critical (objects)
    {
      if (!objects.empty()) {
        object = objects.back();
        objects.pop_back();
        --object_count;
        has_object = true;
      }
    }
    if (has_object) {
      lambda(object);
    }

#pragma omp master
    {
      while (more_input && object_count < read_threshold) {
        handle_count(count);
        string s;
        for (uint64_t i = 0; i < count; ++i) {
          uint32_t msgSize = 0;
          // the messages are prefixed by their size
          delete coded_in;
          coded_in = new protobuf::io::CodedInputStream(gzip_in);
          coded_in->ReadVarint32(&msgSize);
          if ((msgSize > 0) && (coded_in->ReadString(&s, msgSize))) {
            T object;
            object.ParseFromString(s);
#pragma omp critical (objects)
            {
              objects.push_front(object);
              ++object_count;
            }
          }
        }
        more_input = coded_in->ReadVarint64((protobuf::uint64*) &count);
      }
    }
#pragma omp critical (objects)
    more_objects = (object_count > 0);

  } while (more_input || more_objects);

  delete coded_in;
  delete gzip_in;
  delete raw_in;

  return !count;
}

template <typename T> bool
for_each_parallel(istream& in, function<void(T&)>& lambda)
{
  function<void(uint64_t)> noop = [](uint64_t) { };
  return for_each_parallel(in, lambda, noop);
}

}  // io
}  // vg

#endif
