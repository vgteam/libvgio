#ifndef VG_IO_REGISTRY_HPP_INCLUDED
#define VG_IO_REGISTRY_HPP_INCLUDED

/**
 * \file registry.hpp
 * Handles bookkeeping for the various data type tags in stream files.
 *
 * TO ADD A PROTOBUF TYPE:
 * - Add a register_protobuf<Type>("TAG") to Registry::register_everything() in registry.cpp
 * TO ADD A NON-PROTOBUF LOADER/SAVER:
 * - Write your own static initialization code somewhere that will end up in your binary.
 * - Make it call Registry::register_loader_saver<Type[, Base[, Base[...]]]>(tag, load_function, save_function)
 */

#include <string>
#include <unordered_map>
#include <typeinfo>
#include <typeindex>
#include <type_traits>
#include <functional>
#include <iostream>
#include <vector>
#include <google/protobuf/util/type_resolver.h>

namespace vg {

namespace io {

using namespace std;

// We define some functional-programming-style types to build the type of a
// loader function.
//
// A loader function takes a callback that loops over incoming messages. It
// then returns a pointer to the loaded object.
//
// The callback loops over incoming messages by itself calling a callback.
//
// This lets you get per-load state (as locals in the outer function) without
// defining a real loader interface and a bunch of implementation child classes
// that are only used in one place.

/// This is the type of a function that can be fed a series of messages
using message_consumer_function_t = function<void(const string&)>;
/// This is the type of a function that can be given a message consumer to feed messages to.
using message_sender_function_t = function<void(const message_consumer_function_t&)>;
/// This is the type of a function that can allocate and load an object of unspecified type from a message source.
using load_function_t = function<void*(const message_sender_function_t&)>;

/// This is the type of a function that can serialize an object of unspecified type to a message consumer.
using save_function_t = function<void(const void*, const message_consumer_function_t&)>;
/// This is the type of a function that can load an object of unspecified type from a bare input stream.
using bare_load_function_t = function<void*(istream&)>;

/// Like above, but keep track of an optional (can be empty) filename (ie where stream comes from)
using bare_load_function_with_filename_t = function<void*(istream&, const string&)>;

/// This is the type of a function that can save an object of unspecified type to a bare output stream.
using bare_save_function_t = function<void(const void*, ostream&)>;


/**
 * We also have an adapter that takes a function from an istream& to a void*
 * object, and runs that in a thread to adapt it to the message consuming shape
 * of interface. It captures the wrapped function by value.
 */
load_function_t wrap_bare_loader(bare_load_function_t istream_loader);

/**
 * This calls the given stream-using callback with a stream that, when written to, calls the given emit_message function.
 * The emit_message function and the stream-using callback will run in different threads.
 */
void with_function_calling_stream(const message_consumer_function_t& emit_message, const function<void(ostream&)>& use_stream);

/**
 * We have an adapter that takes a function of void* and ostream&, and adapts that
 * to a message consumer destination.
 */
save_function_t wrap_bare_saver(function<void(const void*, ostream&)> ostream_saver);

/**
 * A registry mapping between tag strings for serialized message groups and the
 * Protobuf types or serialization/deserialization handlers to use for them.
 * All registration anmd lookup is done through static methods.
 * Static methods are safe to call from other static initialization code.
 *
 * We handle two kinds of registration right now:
 * - Registration of Protobuf types so we can tag them appropriately on serialization and
 *   detect/skip mismatches on deserialization
 * - Registration of Loader implementations for chunked binary files, so we can
 *   select the appropriate implementation if we are asked to read a HandleGraph
 *   or an LCP or whatever from a stream file.
 */
class Registry {
public:

    /////////
    // Registration API
    /////////
    
    /**
     * Register everything. Main entry point. When you have new things to
     * register, add calls to your function to this method. Returns true on
     * success.
     */
    static bool register_everything();

    /**
     * Register a Protobuf Message type to be associated with the given string
     * tag. By default, Protobuf types use the name string from the Protobuf
     * library as a tag. But this can and should be overridden with something
     * short that will never change.
     */
    template<typename Message>
    static void register_protobuf(const string& tag);
    
    /**
     * Register a loading function and a saving function with the given tag
     * for the given object type and the given base classes.
     */
    template<typename Handled, typename... Bases>
    static void register_loader_saver(const string& tag, load_function_t loader, save_function_t saver);
    
    /**
     * Register a loading function and a saving function with the given
     * collection of tags for the given object type and list of base classes.
     * The first tag in the collection will be used for saving. If "" appears
     * in the list of tags, the loader can be deployed on untagged message
     * groups (for backward compatibility).
     */
    template<typename Handled, typename... Bases>
    static void register_loader_saver(const std::vector<std::string>& tags, load_function_t loader, save_function_t saver);
    
    /**
     * Register a loading function and a saving function with the given tag for
     * the given object type and list of base classes. The functions operate on
     * bare streams; conversion to type-tagged messages of chunks of stream
     * data is performed automatically.
     *
     * Because, without a magic value or header check function, we can't reject
     * type-tagged message files, loaders registered with this function won't
     * actually be used when a non-type-tagged file is loaded. See
     * register_bare_loader_saver_with_magic(), or use
     * register_bare_loader_saver_with_header_check() with an always true check
     * function if you do not need to load type-tagged messages.
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader_saver(const string& tag, bare_load_function_t loader, bare_save_function_t saver);
    
    /**
     * Like register_bare_loader_saver(), except that additionally the function
     * will be used when attempting to load any of the base classes when the
     * file begins with the specified magic bytes. Type-tagged files with the
     * given tag that do not match the magic will be decoded into a stream and
     * loaded with the loader.
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader_saver_with_magic(const string& tag, const string& magic,
        bare_load_function_t loader, bare_save_function_t saver);
        
    /**
     * Like register_bare_loader_saver_with_magic(), except that multiple magic
     * values are supported.
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader_saver_with_magics(const string& tag, const vector<string>& magics,
        bare_load_function_t loader, bare_save_function_t saver);

    /**
     * Generealization of register_bare_loader_saver_with_magic, where user can supply their
     * own header-checking function. Check function must peek/get/unget and may not seek/tell. 
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader_saver_with_header_check(const string& tag,
        function<bool(istream&)> sniff_header,
        bare_load_function_with_filename_t loader, bare_save_function_t saver);    

    /**
     * Generalization of register_bare_loader_saver_with_magic that can also take a filename
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader_saver_with_magic_and_filename(const string& tag, const string& magic,
        bare_load_function_with_filename_t loader, bare_save_function_t saver);
        
    /**
     * Register a load function for a tag. The empty tag means it can run on
     * untagged message groups. If any Bases are passed, we will use this
     * loader to load when one of those types is requested and this tag is
     * encountered.
     */
    template<typename Handled, typename... Bases>
    static void register_loader(const string& tag, load_function_t loader);
    
    /**
     * Register a loading function for the given collection of tags. If ""
     * appears in the list of tags, the loader can be deployed on untagged
     * message groups (for backward compatibility).
     */
    template<typename Handled, typename... Bases>
    static void register_loader(const std::vector<std::string>& tags, load_function_t loader);
    
    /**
     * To help with telling messages from tags, we enforce a max tag length.
     * If we ever allow tags of 139 bytes or longer, we risk having
     * uncompressed files starting with the gzip magic number.
     */
    static const size_t MAX_TAG_LENGTH = 25;
    
    /////////
    // Lookup API
    /////////
    
    /**
     * Determine if the given tag string loaded from a file is a valid/possible
     * tag, or if it should be interpreted as message data from a pre-tags VG
     * file instead. Only tag values literally registered with the registry are
     * valid.
     * NOT thread-safe to run simultaneously with tag registrations.
     */
    static bool is_valid_tag(const string& tag);
    
    /**
     * Get the correct tag to use when serializing Protobuf messages of the
     * given type.
     */
    template<typename Message>
    static const string& get_protobuf_tag();

    /**
     * Check to see if the given tag is expected when deserializing Protobuf
     * messages of the given tag.
     */
    template<typename Message>
    static bool check_protobuf_tag(const string& tag);

    /**
     * Return true of the given stream starts with the given magic number
     * prefix, and false otherwise. Returns the stream to its initial position
     * regardless of the result.
     */
    static bool sniff_magic(istream& stream, const string& magic);

    /**
     * Look up the appropriate loader function to use to load an object of the
     * given type from data with the given tag. If there is one registered,
     * return a pointer to it. The caller has to call it and cast the result to
     * the right type. If there isn't, returns nullptr.
     */
    template<typename Want>
    static const load_function_t* find_loader(const string& tag);
    
    /**
     * Look up the appropriate loader functions to use to load an object of the
     * given type from a bare stream. If there are any registered, return a
     * pointer to a vector of functions and their possibly empty stream
     * prefixes that they require. The caller has to call the appropriate
     * function and and cast the result to the right type. If there are no
     * functions available, returns nullptr.
     */
    template<typename Want>
    static const vector<pair<bare_load_function_with_filename_t, function<bool(istream&)>>>* find_bare_loaders();
    
    /**
     * Look up the appropriate saver function to use to save an object of the
     * given type. If there is one registered, return a pointer to a pair of
     * the tag to use and the function. The caller has to call it and cast the
     * result to the right type. If there isn't, returns nullptr.
     */
    template<typename Have>
    static const pair<string, save_function_t>* find_saver();
    
private:
    
    /**
     * Holds the actual singleton registry tables.
     */
    struct Tables {
        /// Maps from tag string to Protobuf type type_index that it indicates, if any.
        unordered_map<string, type_index> tag_to_protobuf;
        /// Maps from Protobuf type type_index back to string tag.
        unordered_map<type_index, string> protobuf_to_tag;
        
        /// Maps from tag to a map from type_index we want to load to a loading
        /// function that can load it from data with that tag.
        unordered_map<string, unordered_map<type_index, load_function_t>> tag_to_loader;
        /// Maps from type to a single tag and save function pair to use when outputting that type.
        unordered_map<type_index, pair<string, save_function_t>> type_to_saver;
        
        /// Maps from type_index we want to load from a old,
        /// non-tagged-message-format file to a list of "bare" loaders that can load the
        /// desired thing from an istream, and their possibly empty required-prefix-sniffer-functions.
        unordered_map<type_index, vector<pair<bare_load_function_with_filename_t, function<bool(istream&)>>>> type_to_bare_loaders;
    };
    
    /**
     * Get or create the registry tables in which things are registerd.
     */
    static Tables& get_tables();
    
    /**
     * Get a single shared Protobuf resolver we will use for the duration of the program.
     * The Protobuf docs say the resolver must be thread safe.
     */
    static ::google::protobuf::util::TypeResolver& get_resolver();
    
    /**
     * Register a load function for loading the given types from
     * demultiplexed streams. If a header sniffing function is provided, the
     * function will also be used on the whole input stream if it matches the
     * function, skipping demultiplexing of type-tagged messages.
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader(bare_load_function_t loader,
                                     function<bool(istream&)> sniff_header = nullptr);

    /**
     * Slight generalization of above that allows filename to be passed alongside the
     * input stream.
     */
    template<typename Handled, typename... Bases>
    static void register_bare_loader_with_filename(bare_load_function_with_filename_t loader,
                                                   function<bool(istream&)> sniff_header = nullptr);
    
    /**
     * Register a save function to save a type with a given tag. The empty tag
     * is not permitted.
     */
    template<typename Handled>
    static void register_saver(const string& tag, save_function_t saver);
    
};

/////////////
// Template implementations
/////////////

template<typename Message>
void Registry::register_protobuf(const string& tag) {
    // Limit tag length
    assert(tag.size() <= MAX_TAG_LENGTH);

    // Get our state
    Tables& tables = get_tables();

    // Register in both directions
    tables.tag_to_protobuf.emplace(tag, type_index(typeid(Message)));
    tables.protobuf_to_tag.emplace(type_index(typeid(Message)), tag);
    
#ifdef debug
    cerr << "Registered " << Message::descriptor()->full_name() << " as " << tag << endl;
#endif
}

template<typename Handled, typename... Bases>
void Registry::register_loader(const string& tag, load_function_t loader) {
    // Limit tag length
    assert(tag.size() <= MAX_TAG_LENGTH);

    // Get our state
    Tables& tables = get_tables();
    
    // Save the loading function to load the given type
    tables.tag_to_loader[tag][type_index(typeid(Handled))] = loader;
    
    // And for the base classes.
    // Expand over all the base types in an initializer list and use an assignment expression to fill a dummy vector.
    std::vector<load_function_t> dummy{(tables.tag_to_loader[tag][type_index(typeid(Bases))] = loader)...};
}

template<typename Handled, typename... Bases>
void Registry::register_loader(const std::vector<std::string>& tags, load_function_t loader) {
    // There must be tags
    assert(!tags.empty());

    for (auto& tag : tags) {
        register_loader<Handled, Bases...>(tag, loader);
    }
}

template<typename Handled, typename... Bases>
void Registry::register_bare_loader(bare_load_function_t loader, function<bool(istream&)> sniff_header) {
    // I'm sorry
    bare_load_function_with_filename_t named_loader = [loader](istream& stream, const string&filename) {
            return loader(stream);
    };

    register_bare_loader_with_filename<Handled, Bases...>(named_loader, sniff_header);
}
    
template<typename Handled, typename... Bases>
void Registry::register_bare_loader_with_filename(bare_load_function_with_filename_t loader, function<bool(istream&)> sniff_header) {
    // Get our state
    Tables& tables = get_tables();
    
    // Save the loading function to load the given type
    tables.type_to_bare_loaders[type_index(typeid(Handled))].emplace_back(loader, sniff_header);

    // And for the base classes.
    // Expand over all the base types in an initializer list and use an assignment expression to fill a dummy vector.
    std::vector<int> dummy{(tables.type_to_bare_loaders[type_index(typeid(Bases))].emplace_back(loader, sniff_header), 0)...};
}

template<typename Handled>
void Registry::register_saver(const string& tag, save_function_t saver) {
    // Limit tag length
    assert(tag.size() <= MAX_TAG_LENGTH);

    // Prohibit the empty tag here.
    assert(!tag.empty());
    
    // Get our state
    Tables& tables = get_tables();
    
    // Save the saving function to save the given type
    tables.type_to_saver.emplace(type_index(typeid(Handled)), make_pair(tag, saver));
}

template<typename Handled, typename... Bases>
void Registry::register_loader_saver(const string& tag, load_function_t loader, save_function_t saver) {
    // Dispatch to the vector implementation
    register_loader_saver<Handled, Bases...>(std::vector<std::string>{tag}, loader, saver);
}

template<typename Handled, typename... Bases>
void Registry::register_loader_saver(const std::vector<std::string>& tags, load_function_t loader, save_function_t saver) {
    // There must be tags
    assert(!tags.empty());

    // The first must be a real tag we can save with
    assert(!tags.front().empty());
    
    for (auto tag : tags) {
        // Limit tag length
        assert(tag.size() <= MAX_TAG_LENGTH);
    }
    
    // The first tag gets the loader and saver
    register_loader<Handled, Bases...>(tags.front(), loader);
    register_saver<Handled>(tags.front(), saver);
    
    for (size_t i = 1; i < tags.size(); i++) {
        // Other tags just get loaders
        register_loader<Handled, Bases...>(tags[i], loader);
    }
}

template<typename Handled, typename... Bases>
void Registry::register_bare_loader_saver(const string& tag, bare_load_function_t loader, bare_save_function_t saver) {

    // Register the type-tagged wrapped functions
    register_loader_saver<Handled, Bases...>(tag, wrap_bare_loader(loader), wrap_bare_saver(saver));
    
    // Register the bare stream loader. Don't match any files; only handle type-tagged messages.
    register_bare_loader<Handled>(loader, nullptr);

}

template<typename Handled, typename... Bases>
void Registry::register_bare_loader_saver_with_magic(const string& tag, const string& magic,
    bare_load_function_t loader, bare_save_function_t saver) {

    // Register with just one magic
    register_bare_loader_saver_with_magics<Handled, Bases...>(tag, vector<string>({magic}), loader, saver);

}

template<typename Handled, typename... Bases>
void Registry::register_bare_loader_saver_with_magics(const string& tag, const vector<string>& magics,
    bare_load_function_t loader, bare_save_function_t saver) {

    // Register the type-tagged wrapped functions
    register_loader_saver<Handled, Bases...>(tag, wrap_bare_loader(loader), wrap_bare_saver(saver));
    
    for (auto& magic : magics) {
        // Register the bare stream loader for each magic
        register_bare_loader<Handled, Bases...>(loader, [magic](istream& in_stream) {
                return sniff_magic(in_stream, magic);  
            });
    }
}

template<typename Handled, typename... Bases>
void Registry::register_bare_loader_saver_with_header_check(const string& tag, function<bool(istream&)> sniff_header,
    bare_load_function_with_filename_t loader, bare_save_function_t saver) {

    // I'm sorry
    bare_load_function_t bare_loader = [loader](istream& stream) {
        return loader(stream, "");
    };
    
    // Register the type-tagged wrapped functions
    register_loader_saver<Handled, Bases...>(tag, wrap_bare_loader(bare_loader), wrap_bare_saver(saver));

    // Register the bare stream loader with header check
    register_bare_loader_with_filename<Handled, Bases...>(loader, sniff_header);
}

template<typename Handled, typename... Bases>
void Registry::register_bare_loader_saver_with_magic_and_filename(const string& tag, const string& magic,
    bare_load_function_with_filename_t loader, bare_save_function_t saver) {

    assert(tag.size() <= MAX_TAG_LENGTH);

    register_bare_loader_with_filename<Handled, Bases...>(loader, [magic](istream& in_stream) {
        return sniff_magic(in_stream, magic);
    });
    register_saver<Handled, Bases...>(tag, wrap_bare_saver(saver));

}
        

template<typename Want>
const load_function_t* Registry::find_loader(const string& tag) {
    
    if (tag.size() > MAX_TAG_LENGTH) {
        // Too long to be correct, and definitely longer than we want to hash.
        return nullptr;
    }

    // Get our state
    Tables& tables = get_tables();
    
    auto found_tag = tables.tag_to_loader.find(tag);
    if (found_tag != tables.tag_to_loader.end()) {
        // We can load this tag to something.
        // Grab the map from type_index to actual loader
        auto& loaders = found_tag->second;
        
        auto found_loader = loaders.find(type_index(typeid(Want)));
        if (found_loader != loaders.end()) {
            // We can load this tag to the requested type.
            // Return a pointer to the function that does it.
            return &found_loader->second;
        }
    }
    
    // We can't find the right function. Return null.
    return nullptr;
}

template<typename Want>
const vector<pair<bare_load_function_with_filename_t, function<bool(istream&)>>>* Registry::find_bare_loaders() {
    // Get our state
    Tables& tables = get_tables();
    
    // Look for a loader for this type from bare streams
    auto found = tables.type_to_bare_loaders.find(type_index(typeid(Want)));
    
    if (found != tables.type_to_bare_loaders.end() && !found->second.empty()) {
        // We found one or more loaders
        return &found->second;
    }
    
    // We don't have any loaders to load this from a bare file.
    return nullptr;
}
    

template<typename Have>
const pair<string, save_function_t>* Registry::find_saver() {
    // Get our state
    Tables& tables = get_tables();
   
    // Look for a saver for this templated type.
    auto found = tables.type_to_saver.find(type_index(typeid(Have)));
    if (found != tables.type_to_saver.end()) {
        // We only have one. Return a pointer to the pair of the tag to apply and the function to call.
        return &found->second;
    }
    
    // Otherwise we didn't find anything to use to save this.
    return nullptr;
}

template<typename Message>
const string& Registry::get_protobuf_tag() {
    // Get our state
    Tables& tables = get_tables();

    // See if we have a tag defined
    auto found = tables.protobuf_to_tag.find(type_index(typeid(Message)));
    
    if (found != tables.protobuf_to_tag.end()) {
        // There is a custom tag registered.
#ifdef debug
        cerr << "Tag found for type " << Message::descriptor()->full_name() << ": " << found->second << endl;
#endif
        return found->second;
    } else {
        // Use the default name from the Protobuf library as a tag
#ifdef debug
        cerr << "Tag not found for " << Message::descriptor()->full_name() << endl;
#endif
        const string& tag = Message::descriptor()->full_name();
        
        // Limit tag length
        assert(tag.size() <= MAX_TAG_LENGTH);
        
        return tag;
    }
}

template<typename Message>
bool Registry::check_protobuf_tag(const string& tag) {
    if (tag.empty()) {
        // For reading old tagless files, "" is always a valid tag for Protobuf data.
        return true;
    }
    
    if (tag.size() > MAX_TAG_LENGTH) {
        // Too long to be correct, and definitely longer than we want to hash.
        return false;
    }

    // Get our state
    Tables& tables = get_tables();
    
    // See if we have a protobuf defined for this tag as an override.
    auto found = tables.tag_to_protobuf.find(tag);
    
    if (found != tables.tag_to_protobuf.end()) {
        // We do have a Protobuf specifically assigned to this tag
        
        // Return true iff it is the same Protobuf type we are checking
        return type_index(typeid(Message)) == found->second;
    } else {
        // Return if the tag is the Protobuf type's fully qualified name
        return tag == Message::descriptor()->full_name();
    }
}

}

}

#endif
