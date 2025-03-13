/**
 * \file message_iterator.cpp
 * Implementations for the MessageIterator for reading type-tagged grouped message files
 */

#include "vg/io/message_iterator.hpp"
#include "vg/io/registry.hpp"

namespace vg {

namespace io {

using namespace std;

// Provide the static values a compilation unit to live in.
const size_t MessageIterator::MAX_MESSAGE_SIZE;

string MessageIterator::sniff_tag(istream& stream) {

    if (!stream) {
        // Can't read anything
        return "";
    }
    
    // Work out how many characters to try and sniff.
    // We need a 64 bit varint, a 32 bit varint, and a max length tag.
    // Varints are stored 7 bits to a byte.
    // We require that our C++ STL can do this much ungetting, even though the
    // standard guarantees absolutely no ungetting.
    size_t to_sniff = 10 + 5 + Registry::MAX_TAG_LENGTH;
    
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
    
    // Make a coded input stream stream over the data we got
    // Default total bytes limit will be fine.
    google::protobuf::io::CodedInputStream coded_in((::google::protobuf::uint8*) buffer, buffer_used);
    
    // Read a message count. 
    size_t group_count;
    if (!coded_in.ReadVarint64((::google::protobuf::uint64*) &group_count) || group_count < 1) {
        // If not 1 or more, stop.
        return "";
    }
    
    // Read a tag length.
    uint32_t tag_size;
    if (!coded_in.ReadVarint32(&tag_size) || tag_size == 0 || tag_size > Registry::MAX_TAG_LENGTH) {
        // If not above 0 and below some sensible limit, stop.
        return "";
    }
    
    // Read the tag data.
    string tag;
    if (!coded_in.ReadString(&tag, tag_size) || tag.size() != tag_size) {
        // If we run out of buffer and don't get the right length, stop.
        return "";
    }
    
    // Check the tag with the registry.
    if (!Registry::is_valid_tag(tag)) {
        // If tag is not valid according to the registry, stop.
        return "";
    }
    
    // Report the tag sniffed.
    return tag;
}

string MessageIterator::sniff_tag(::google::protobuf::io::ZeroCopyInputStream& stream) {

    // Get the buffer of data available.
    char* buffer = nullptr;
    // And the number of bytes in it.
    int did_sniff = 0;
    if (!stream.Next((const void**)&buffer, &did_sniff)) {
        // Could not read anything.
        return "";
    }

    // Make a coded input stream stream over the data we got
    // Default total bytes limit will be fine.
    google::protobuf::io::CodedInputStream coded_in((::google::protobuf::uint8*) buffer, did_sniff);
    
    // Read a message count. 
    size_t group_count;
    if (!coded_in.ReadVarint64((::google::protobuf::uint64*) &group_count) || group_count < 1) {
        // If not 1 or more, stop.
        stream.BackUp(did_sniff);
        return "";
    }
    
    // Read a tag length.
    uint32_t tag_size;
    if (!coded_in.ReadVarint32(&tag_size) || tag_size == 0 || tag_size > Registry::MAX_TAG_LENGTH) {
        // If not above 0 and below some sensible limit, stop.
        stream.BackUp(did_sniff);
        return "";
    }
    
    // Read the tag data.
    string tag;
    if (!coded_in.ReadString(&tag, tag_size) || tag.size() != tag_size) {
        // If we run out of buffer and don't get the right length, stop.
        stream.BackUp(did_sniff);
        return "";
    }
    
    // Check the tag with the registry.
    if (!Registry::is_valid_tag(tag)) {
        // If tag is not valid according to the registry, stop.
        stream.BackUp(did_sniff);
        return "";
    }
    
    // Report the tag sniffed.
    stream.BackUp(did_sniff);
    return tag;
}

MessageIterator::MessageIterator(istream& in, bool verbose, size_t thread_count) : MessageIterator(unique_ptr<BlockedGzipInputStream>(new BlockedGzipInputStream(in, thread_count)), verbose) {
    // Nothing to do!
}

MessageIterator::MessageIterator(unique_ptr<BlockedGzipInputStream>&& bgzf, bool verbose) :
    value(),
    previous_tag(),
    group_count(0),
    group_idx(0),
    group_vo(-1),
    item_vo(-1),
    bgzip_in(std::move(bgzf)),
    verbose(verbose)
{
    advance();
}

auto MessageIterator::operator*() const -> const TaggedMessage& {
    return value;
}

auto MessageIterator::operator*() -> TaggedMessage& {
    return value;
}


auto MessageIterator::operator++() -> const MessageIterator& {
    while (group_count == group_idx) {
        // We have made it to the end of the group we are reading. We will
        // start a new group now (and skip through empty groups).
        
        // Determine exactly where we are positioned, if possible, before
        // creating the CodedInputStream to read the group's item count
        auto virtual_offset = bgzip_in->Tell();
        
        if (virtual_offset == -1) {
            // We don't have seek capability, so we just count up the groups we read.
            // On construction this is -1; bump it up to 0 for the first group.
            group_vo++;
        } else {
            // We can seek. We need to know what offset we are at
            group_vo = virtual_offset;
        }
        
        // Start at the start of the new group
        group_idx = 0;
        
        // Make a CodedInputStream to read the group length
        ::google::protobuf::io::CodedInputStream coded_in(bgzip_in.get());
        // Alot space for group's length, tag's length, and tag (generously)
        coded_in.SetTotalBytesLimit(MAX_MESSAGE_SIZE * 2);
        
        // Try and read the group's length
        if (!coded_in.ReadVarint64((::google::protobuf::uint64*) &group_count)) {
            // We didn't get a length
            
            if (this->verbose) {
                cerr << "Failed to read group count at " << group_vo << "; stop iteration." << endl;
            }
            
            // This is the end of the input stream, switch to state that
            // will match the end constructor
            group_vo = -1;
            item_vo = -1;
            value.first.clear();
            value.second.reset();
            return *this;
        }
        
        if (this->verbose) {
            cerr << "Read group count at " << group_vo << ": " << group_count << endl;
        }
        
        // Now we have to grab the tag, which is pretending to be the first item.
        // It could also be the first item, if it isn't a known tag string.
        
        // Get the tag's virtual offset, if available
        virtual_offset = bgzip_in->Tell();
        
        // The tag is prefixed by its size
        uint32_t tag_size = 0;
        handle(coded_in.ReadVarint32(&tag_size), group_vo);
        
        if (tag_size > MAX_MESSAGE_SIZE) {
            throw runtime_error("[vg::io::MessageIterator::operator++] (group " + 
                                to_string(group_vo) + ") tag of " +
                                to_string(tag_size) + " bytes is too long");
        }
        
        // Read it into the tag field of our value
        value.first.clear();
        if (tag_size) {
            handle(coded_in.ReadString(&value.first, tag_size), group_vo);
        }
        
        if (this->verbose) {
            cerr << "Read what should be the tag of " << tag_size << " bytes" << endl;
        }
        
        // Update the counters for the tag, which the counters treat as a message.
        if (virtual_offset == -1) {
            // Just track the counter.
            item_vo++;
        } else {
            // We know where here is
            item_vo = virtual_offset;
        }
        
        // Move on to the next message in the group
        group_idx++;
    
        // Work out if this really is a tag.
        bool is_tag = false;
        
        if (!previous_tag.empty() && previous_tag == value.first) {
            if (this->verbose) {
                cerr << "Tag is the same as the last tag of \"" << previous_tag << "\"" << endl;
            }
            is_tag = true;
        } else {
            if (this->verbose) {
                cerr << "Tag does not match cached previous tag or there is no cached previous tag" << endl;
            }
        }
    
        if (!is_tag && Registry::is_valid_tag(value.first)) {
            if (this->verbose) {
                cerr << "Tag \"" << value.first << "\" is OK with the registry" << endl;
            }
            is_tag = true;
        } else if (!is_tag) {
            if (this->verbose) {
                cerr << "Tag is not approved by the registry" << endl;
            }
        }
    
        if (!is_tag) {
            // If we get here, the registry doesn't think it's a tag.
            // Assume it is actually a message, and make the group's tag ""
            value.second = make_unique<string>(std::move(value.first));
            value.first.clear();
            previous_tag.clear();
            
            if (this->verbose) {
                cerr << "Tag is actually a message probably." << endl;
                cerr << "Found message with tag \"" << value.first << "\"" << endl;
            }
            
            // Return ourselves, after increment
            return *this;
        }
        
        // Otherwise this is a real tag.
        // Back up its value in case our pair gets moved away.
        previous_tag = value.first;
        
        if (is_tag && group_count == 1) {
            // This group is a tag *only*.
            // If we hit the end of the loop we'll just skip over it.
            // We want to emit it as a pair of (tag, null).
            // So we consider our increment complete here.
            
            if (this->verbose) {
                cerr << "Found message-less tag \"" << value.first << "\"" << endl;
            }
            
            value.second.reset();
            return *this;
        }
        
        // We continue through all empty groups.
    }
    
    // Now we know we have a message to go with our tag.
    
    // Now we know we're in a group, and we know the tag, if any.
    
    // Get the item's virtual offset, if available
    auto virtual_offset = bgzip_in->Tell();
    
    // We need a fresh CodedInputStream every time, because of the total byte limit
    ::google::protobuf::io::CodedInputStream coded_in(bgzip_in.get());
    // Alot space for size and item (generously)
    coded_in.SetTotalBytesLimit(MAX_MESSAGE_SIZE * 2);
    
    // A message starts here
    if (virtual_offset == -1) {
        // Just track the counter.
        item_vo++;
    } else {
        // We know where here is
        item_vo = virtual_offset;
    }
    
    // The messages are prefixed by their size
    uint32_t msgSize = 0;
    handle(coded_in.ReadVarint32(&msgSize), group_vo, item_vo);
    
    if (msgSize > MAX_MESSAGE_SIZE) {
        throw runtime_error("[vg::io::MessageIterator::operator++] (group " + 
                            to_string(group_vo) + ") message of " +
                            to_string(msgSize) + " bytes is too long");
    }
    
    
    // We have a message.
    // Make an empty string to hold it.
    if (value.second.get() != nullptr) {
        value.second->clear();
    } else {
        value.second = make_unique<string>();
    }
    if (msgSize) {
        handle(coded_in.ReadString(value.second.get(), msgSize), group_vo, item_vo);
    }
    
    // Fill in the tag from the previous to make sure our value pair actually has it.
    // It may have been moved away.
    value.first = previous_tag;
    
    if (this->verbose) {
        cerr << "Found message " << group_idx << " size " << msgSize << " with tag \"" << value.first << "\"" << endl;
    }
    
    // Move on to the next message in the group
    group_idx++;
    
    // Return ourselves, after increment
    return *this;
}

auto MessageIterator::operator==(const MessageIterator& other) const -> bool {
    // Just ask if we both agree on whether we hit the end.
    return has_current() == other.has_current();
}
    
auto MessageIterator::operator!=(const MessageIterator& other) const -> bool {
    // Just ask if we disagree on whether we hit the end.
    return has_current() != other.has_current();
}

auto MessageIterator::has_current() const -> bool {
    return item_vo != -1;
}

auto MessageIterator::advance() -> void {
    // Run increment but don't return anything.
    ++(*this);
}

auto MessageIterator::take() -> TaggedMessage {
    auto temp = std::move(value);
    advance();
    // Return by value, which gets moved.
    return temp;
}

auto MessageIterator::tell_group() const -> int64_t {
    if (bgzip_in->Tell() != -1) {
        // The backing file supports seek/tell (which we ascertain by attempting it).
        if (group_vo == -1) {
            // We hit EOF and have no loaded message
            return bgzip_in->Tell();
        } else {
            // Return the *group's* virtual offset (not the current one)
            return group_vo;
        }
    } else {
        // group_vo holds a count. But we need to say we can't seek.
        return -1;
    }
}

auto MessageIterator::seek_group(int64_t virtual_offset) -> bool {
    if (virtual_offset < 0) {
        // That's not allowed
        if (this->verbose) {
            cerr << "Can't seek to negative position" << endl;
        }
        return false;
    }
    
    if (group_idx == 0 && group_vo == virtual_offset) {
        // We are there already
        if (this->verbose) {
            cerr << "Already at seek position" << endl;
        }
        return true;
    }
    
    // Try and do the seek
    bool sought = bgzip_in->Seek(virtual_offset);
    
    if (!sought) {
        // We can't seek
        if (this->verbose) {
            cerr << "bgzip_in could not seek" << endl;
        }
        return false;
    }
    
    // Get ready to read the group that's here
    group_count = 0;
    group_idx = 0;
    
    if (this->verbose) {
        cerr << "Successfully sought" << endl;
    }
    
    // Read it (or detect EOF)
    advance();
    
    // It worked!
    return true;
}

auto MessageIterator::range(istream& in) -> pair<MessageIterator, MessageIterator> {
    return make_pair(MessageIterator(in), MessageIterator());
}

auto MessageIterator::handle(bool ok, int64_t group_virtual_offset, int64_t message_virtual_offset) -> void {
    if (!ok) {
        if (message_virtual_offset) {
            throw runtime_error("[vg::io::MessageIterator] obsolete, invalid, or corrupt input at message " +
                to_string(message_virtual_offset) + " group " + to_string(group_virtual_offset));
        } else {
            throw runtime_error("[vg::io::MessageIterator] obsolete, invalid, or corrupt input at group " +
                to_string(group_virtual_offset));
        }
    }
}

}

}
