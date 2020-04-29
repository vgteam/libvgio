/**
 * \file message_emitter.cpp
 * Implementations for the MessageEmitter.
 */

#include "vg/io/message_emitter.hpp"

namespace vg {

namespace io {

using namespace std;

// Give the static member variable a .o home
const size_t MessageEmitter::MAX_MESSAGE_SIZE = 1000000000;

MessageEmitter::MessageEmitter(ostream& out, bool compress, size_t max_group_size) :
    group(),
    max_group_size(max_group_size),
    bgzip_out(compress ? new BlockedGzipOutputStream(out) : nullptr),
    uncompressed_out(compress ? nullptr : new google::protobuf::io::OstreamOutputStream(&out)),
    uncompressed_out_ostream(compress ? nullptr : &out),
    uncompressed_out_written(0)
{
#ifdef debug
    cerr << "Creating MessageEmitter" << endl;
#endif

    // GZIP magic number is: 0x1F 0x8B
    // Protobuf varint encoding is: break into groups of 7 bits, send them out least-significant first, set high bit on all but the last.
    // Each type-tagged message file starts with the number of items (including tag) in its group.
    // GZIP magic number decoded like this looks like a group of 31 messages with an initial message (tag) with >=139 bytes.
    // So as long as we enforce a tag length of < that we won't have a problem.
    
    if (bgzip_out.get() != nullptr && bgzip_out->Tell() == -1) {
        // Say we are starting at the beginning of the stream, if we don't know where we are.
        bgzip_out->StartFile();
    }
}

MessageEmitter::~MessageEmitter() {
#ifdef debug
    cerr << "Destroying MessageEmitter" << endl;
#endif
    
    if (bgzip_out.get() != nullptr || uncompressed_out.get() != nullptr) {
#ifdef debug
        cerr << "MessageEmitter emitting final group" << endl;
#endif
    
        // Before we are destroyed, write stuff out.
        emit_group();
    }

    if (bgzip_out.get() != nullptr) {
#ifdef debug
        cerr << "MessageEmitter ending file" << endl;
#endif
        
        // Tell our stream to finish the file (since it hasn't been moved away)
        bgzip_out->EndFile();
    }
    
#ifdef debug
    cerr << "MessageEmitter destroyed" << endl;
#endif
}

void MessageEmitter::write(const string& tag) {
    if (group.size() >= max_group_size || tag != group_tag) {
        // We have run out of buffer space or changed type
        emit_group();
    }
    if (tag != group_tag) {
        // Adopt the new tag
        group_tag = tag;
#ifdef debug
        cerr << "Adopting tag " << group_tag << endl;
#endif
    }
}

void MessageEmitter::write(const string& tag, string&& message) {
    // Ensure the current group is for the given tag
    write(tag);
    group.emplace_back(std::move(message));
    
    if (group.back().size() > MAX_MESSAGE_SIZE) {
        throw std::runtime_error("io::MessageEmitter::write: message too large");
    }
}

void MessageEmitter::write_copy(const string& tag, const string& message) {
    // Ensure the current group is for the given tag
    write(tag);
    group.push_back(message);
    
    if (group.back().size() > MAX_MESSAGE_SIZE) {
        throw std::runtime_error("io::MessageEmitter::write_copy: message too large");
    }
}

void MessageEmitter::on_group(group_listener_t&& listener) {
    group_handlers.emplace_back(std::move(listener));
}

void MessageEmitter::emit_group() {
    if (group_tag.empty()) {
        // Nothing have been loaded into our buffer, not even an empty group with a tag.
        return;
    }
    // If we have a tag, we emit a group, even if it is empty.
    
    
    // We can't write a non-empty buffer if our stream is gone/moved away
    assert(bgzip_out.get() != nullptr || uncompressed_out.get() != nullptr);
    
    auto handle = [](bool ok) {
        if (!ok) {
            throw std::runtime_error("io::MessageEmitter::emit_group: I/O error writing protobuf");
        }
    };

    // Work out where the group we emit will start
    int64_t virtual_offset = (bgzip_out.get() != nullptr) ? bgzip_out->Tell() : (uncompressed_out_written + uncompressed_out->ByteCount());

    {
        // Make a CodedOutput Stream that we will clean up (to flush) before we give up control.
        ::google::protobuf::io::CodedOutputStream coded_out((bgzip_out.get() != nullptr) ? 
            (::google::protobuf::io::ZeroCopyOutputStream*) bgzip_out.get() :
            (::google::protobuf::io::ZeroCopyOutputStream*) uncompressed_out.get());

#ifdef debug
        cerr << "Writing group size of " << (group.size() + 1) << endl;
#endif

        // Prefix the group with the number of objects, plus 1 for the tag header
        coded_out.WriteVarint64(group.size() + 1);
        handle(!coded_out.HadError());
       
#ifdef debug
        cerr << "Writing tag " << group_tag << endl;
#endif
       
        // Write the tag length and the tag
        coded_out.WriteVarint32(group_tag.size());
        handle(!coded_out.HadError());
        coded_out.WriteRaw(group_tag.data(), group_tag.size());
        handle(!coded_out.HadError());

        for (auto& message : group) {
            
#ifdef debug
            cerr << "Writing message of " << message.size() << " bytes in group of \""
                << group_tag << "\" @ " << virtual_offset << endl;
#endif
            
            // And prefix each object with its size
            coded_out.WriteVarint32(message.size());
            handle(!coded_out.HadError());
            coded_out.WriteString(message);
            handle(!coded_out.HadError());
        }
        
        coded_out.Trim();
    }
    
    // Work out where we ended
    int64_t next_virtual_offset = (bgzip_out.get() != nullptr) ? bgzip_out->Tell() : (uncompressed_out_written + uncompressed_out->ByteCount());
    
    if (uncompressed_out.get() != nullptr) {
#ifdef debug
        cerr << "Protobuf has written " << uncompressed_out->ByteCount() << " and stream has written " << uncompressed_out_ostream->tellp() << endl;
#endif
        // We have to have written something
        assert(uncompressed_out->ByteCount() > 0);
    }
    
    for (auto& handler : group_handlers) {
        // Report the group to each group handler that is listening.
        // We don't report the individual messages. They need to be observed separately.
        handler(group_tag, virtual_offset, next_virtual_offset);
    }
    
    // Empty the buffer because everything in it is written
    group.clear();
    
    // Clear the tag out because now nothing is buffered.
    group_tag.clear();
}

void MessageEmitter::flush() {
    // Make sure to emit our group, if any.
    emit_group();
    
    assert(bgzip_out.get() != nullptr || uncompressed_out.get() != nullptr);
    if (bgzip_out.get() != nullptr) {
#ifdef debug
        cerr << "Flushing MessageEmitter to BlockedGzipOutputStream" << endl;
#endif

        bgzip_out->Flush();
    }
    if (uncompressed_out.get() != nullptr) {
    
#ifdef debug
        cerr << "Flushing MessageEmitter to OstreamOutputStream" << endl;
#endif
    
        // Remember how much was written from this stream
        uncompressed_out_written += uncompressed_out->ByteCount();
        // Destroy and recreate it to empty its buffer.
        // Do a separate reset first to make sure only one OstreamOutputStream can be talking to the stream at a time.
        // TODO: Probably overly cautious.
        uncompressed_out.reset(nullptr);
        uncompressed_out.reset(new google::protobuf::io::OstreamOutputStream(uncompressed_out_ostream));
        
#ifdef debug
        cerr << "Stream has written " << uncompressed_out_ostream->tellp() << endl;
#endif
        
    }
}

}

}
