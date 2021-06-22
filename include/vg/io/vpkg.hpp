#ifndef VG_IO_VPKG_HPP_INCLUDED
#define VG_IO_VPKG_HPP_INCLUDED

/**
 * \file vpkg.hpp: frontend load/save interface for multi-type type-tagged files
 */

#include "registry.hpp"
#include "message_iterator.hpp"
#include "message_emitter.hpp"
#include "fdstream.hpp"

#include <iostream>
#include <tuple>
#include <vector>
#include <deque>
#include <memory>

// We use this for fancy type-name demangling. Hopefully the compiler has it.
#include <cxxabi.h>

namespace vg {

namespace io {

using namespace std;

/**
 * Interface for reading/writing files.
 *
 * Originally designed for Protobuf-based type-tagged files with optional BGZIP
 * compression.
 *
 * Now mostly used for files identified by prefixes or sniffing functions.
 *
 * Allows you to load e.g. a HandleGraph from a file with the implementation
 * being auto-selected based on what is in the file.
 */
class VPKG {
public:
    
    /**
     * Allocate and load an available type from the given stream.
     * Returns a tuple where at most one item is filled, and that item is the
     * first type we could successfully load out of the template parameters,
     * except that bare loaders are prioritized over encapsulated ones.
     *
     * filename is optional, and can be used by callback code that may, for
     * some reason, want to link the stream back to a path (cough cough GFA
     * loader cough cough)
     */
    template<typename... Wanted>
    static tuple<unique_ptr<Wanted>...> try_load_first(istream& in, const string& filename = "") {
        // We will fill this in if we manage to load the thing
        tuple<unique_ptr<Wanted>...> results;
        
        // We can't reduce over the eldritch abomination that is a tuple of a
        // variable number of variable-typed things without making a pact with
        // the template gods, so we just set this flag as soon as we find one
        // of the options so we can stop looking for the others.
        bool found = false;
        
        // Use one unified unget setup to not consume any stream in the subcalls.
        with_putback(in, [&](istream& from) {
            // First try a bare loader
            results = std::move(try_load_first_bare<Wanted...>(from, filename, found));
            
            // The bare loaders will always unget back to the start.
            
            if (!found) {
                // If that doesn't work, try loading as encapsulated type-tagged data
                
                // Make an iterator to read it
                MessageIterator it(from);
                
                // And read it
                results = std::move(try_load_first_encapsulated<Wanted...>(it, found));
            }
        });
        
        return std::move(results);
    }
    
    /**
     * Allocate and load the first available type from the given stream.
     * Returns a tuple where at most one item is filled, and that item is the
     * first type we could successfully load out of the template parameters.
     *
     * If any type is to be loaded from type-tagged messages, it must come
     * first, or its type-tagged messages may be skipped when looking for
     * higher-priority types.
     */
    template<typename... Wanted>
    static tuple<unique_ptr<Wanted>...> try_load_first(const string& filename) {
        if (filename.empty()) {
            // There's no file here, so fail by returning an empty tuple.
            return tuple<unique_ptr<Wanted>...>();
        }
        
        if (filename == "-") {
            return try_load_first<Wanted...>(cin);
        } else {
            // Open the file
            ifstream open_file(filename.c_str());
            
            if (!open_file) {
                cerr << "error[VPKG::load_one]: Could not open " << filename << " to determine file type" << endl;
                exit(1);
            }
            
            // Read from it
            return try_load_first<Wanted...>(open_file, filename);
        }
    }
    
    /**
     * Load an object of the given type from a file.
     * The stream may be VPKG with the appropriate tag, or a bare non-VPKG
     * stream understood by the loader.
     *
     * Tagged messages that can't be used to load the thing we are looking for
     * may or may not be consumed, so you should not call this function in a
     * loop over types on the same stream.
     *
     * filename is optional, and can be used by callback code that may, for
     * some reason, want to link the stream back to a path (cough cough GFA
     * loader cough cough) 
     */
    template<typename Wanted>
    static unique_ptr<Wanted> try_load_one(istream& in, const string& filename = "") {
        // We will fill this in if we manage to load the thing
        unique_ptr<Wanted> result;
        
        // Use one unified unget setup to not consume any stream in the subcalls.
        with_putback(in, [&](istream& from) {
            // First try a bare loader
            result = std::move(try_load_bare<Wanted>(from, filename));
            
            if (!result) {
                // If that doesn't work, try loading as encapsulated type-tagged data
                result = std::move(try_load_encapsulated<Wanted>(from));
            }
        });
        
        return result;
     }
    
    /**
     * Load an object of the given type from a file by name.
     * The stream may be VPKG with the appropriate tag, or a bare non-VPKG stream understood by the loader.
     * Returns null if the object could not be found in the file. Supports "-" for standard input.
     */
    template<typename Wanted>
    static unique_ptr<Wanted> try_load_one(const string& filename) {
        if (filename.empty()) {
            // There's no file here, so fail by returning an empty pointer.
            return unique_ptr<Wanted>();
        }
        
        if (filename == "-") {
            return try_load_one<Wanted>(cin, "");
        } else {
            // Open the file
            ifstream open_file(filename.c_str());
            
            // Read from it
            return try_load_one<Wanted>(open_file, filename);
        }
    }
    
    /**
     * Load an object of the given type from a stream.
     * The stream may be VPKG with the appropriate tag, or a bare non-VPKG stream understood by the loader.
     * Tagged messages that can't be used to load the thing we are looking for are skipped.
     * Ends the program with an error if the object could not be found in the stream.
     *
     * May consume trailing data from the stream.
     */
    template<typename Wanted>
    static unique_ptr<Wanted> load_one(istream& in, const string& filename = "") {
        if (!in) {
            cerr << "error[VPKG::load_one]: Unreadable stream while loading " << describe<Wanted>() << endl;
            exit(1);
        }

        // Read from it
        auto result = try_load_one<Wanted>(in, filename);
        
        if (result.get() == nullptr) {
            cerr << "error[VPKG::load_one]: Correct input type not found while loading " << describe<Wanted>() << endl;
            exit(1);
        }
        
        return result;
    }
    
    /**
     * Load an object of the given type from a file by name.
     * The stream may be VPKG with the appropriate tag, or a bare non-VPKG stream understood by the loader.
     * Tagged messages that can't be used to load the thing we are looking for are skipped.
     * Ends the program with an error if the object could not be found in the file. Supports "-" for standard input.
     */
    template<typename Wanted>
    static unique_ptr<Wanted> load_one(const string& filename) {
        if (filename.empty()) {
            cerr << "error[VPKG::load_one]: File name missing wile loading " << describe<Wanted>() << endl;
            exit(1);
        }
        
        // We branch into two completely different flows here for better error reporting.
        
        if (filename == "-") {
            // Load from cin
            if (!cin) {
                cerr << "error[VPKG::load_one]: Could not access standard input while loading " << describe<Wanted>() << endl;
                exit(1);
            }
            
            // Read the stream.
            auto result = try_load_one<Wanted>(cin);
            
            if (result.get() == nullptr) {
                cerr << "error[VPKG::load_one]: Correct input type not found in standard input while loading " << describe<Wanted>() << endl;
                exit(1);
            }
            
            return result;
            
        } else {
            // Load from a real file
            ifstream in(filename);
            
            if (!in) {
                // We can't even open the file
                cerr << "error[VPKG::load_one]: Could not open " << filename << " while loading " << describe<Wanted>() << endl;
                exit(1);
            }
            
            // Read the file.
            auto result = try_load_one<Wanted>(in, filename);
            
            if (result.get() == nullptr) {
                cerr << "error[VPKG::load_one]: Correct input type not found in " << filename << " while loading " << describe<Wanted>() << endl;
                exit(1);
            }
            
            return result;
        }
    }
    
    
    
    /**
     * Save an object to the given stream, using the appropriate saver.
     */
    template<typename Have>
    static void save(const Have& have, ostream& out) {
        // Look for a saver in the registry
        auto* tag_and_saver = Registry::find_saver<Have>();
        
        // We shouldn't ever be saving something we don't know how to save.
        assert(tag_and_saver != nullptr);
        
#ifdef debug
        cerr << "Saving " << describe<Have>() << " to stream with tag " << tag_and_saver->first << endl;
#endif
        
        if (!out) {
            cerr << "error[VPKG::save]: Could not write to stream while saving " << describe<Have>() << endl;
            exit(1);
        }
        
        // Make an emitter to emit tagged messages
        MessageEmitter emitter(out);
        
        // Mark that we serialized something with this tag, even if there aren't actually any messages.
        emitter.write(tag_and_saver->first);
        
        // Start the save
        tag_and_saver->second((const void*)&have, [&](const string& message) {
            // For each message that we have to output during the save, output it via the emitter with the selected tag.
            // TODO: We copy the data string.
            emitter.write_copy(tag_and_saver->first, message);
        });
    }
    
    /*
     * Save an object to the given filename, using the appropriate saver.
     * Supports "-" for standard output.
     */
    template<typename Have>
    static void save(const Have& have, const string& filename) {
        if (filename == "-") {
            save<Have>(have, cout);
        } else {
            // Open the file
            ofstream open_file(filename.c_str());
            
            if (!open_file) {
                cerr << "error[VPKG::save]: Could not open " << filename << " while saving " << describe<Have>() << endl;
                exit(1);
            }
            
            // Save to it
            save<Have>(have, open_file);
        }
    }
    
    /**
     * Lower-level function used to get direct access to a stream tagged with
     * the given tag, in the given type-tagged message output file.
     */
    static void with_save_stream(ostream& to, const string& tag, const function<void(ostream&)>& use_stream);
    
private:

    /**
     * Allocate and load the first available type from the given stream, using
     * a bare loader. Returns a tuple where at most one item is filled, and
     * that item is the first type we could successfully load out of the
     * template parameters. Sets found to true if anything is successfully loaded.
     *
     * filename is optional, and can be used by callback code that may, for
     * some reason, want to link the stream back to a path (cough cough GFA
     * loader cough cough)
     */
    template<typename FirstPriority, typename SecondPriority, typename... Rest>
    static tuple<unique_ptr<FirstPriority>, unique_ptr<SecondPriority>, unique_ptr<Rest>...> try_load_first_bare(istream& in, const string& filename, bool& found) {
    
        // We will load to here if we can
        tuple<unique_ptr<FirstPriority>> first_result;
        // And to here if we can't
        tuple<unique_ptr<SecondPriority>, unique_ptr<Rest>...> remaining_results;
        
        // Use one unified unget setup to not consume any stream in the subcalls.
        with_putback(in, [&](istream& from) {
           
            // Try and load the best thing. If we find a weird VPKG tag, don't skip over it.
            get<0>(first_result) = std::move(try_load_bare<FirstPriority>(in, filename));
            
            if (get<0>(first_result)) {
                // We found it
                found = true;
            } else {
                // It wasn't there.
                
                // Try the (nonempty) others
                remaining_results = std::move(try_load_first_bare<SecondPriority, Rest...>(in, filename, found));
            }
        });
        
        return tuple_cat(std::move(first_result), std::move(remaining_results));
    }
    
    /**
     * Allocate and load the first available type from the given stream, using
     * a bare loader. Returns a tuple where at most one item is filled, and
     * that item is the first type we could successfully load out of the
     * template parameters. Sets found to true if anything is successfully loaded.
     *
     * filename is optional, and can be used by callback code that may, for
     * some reason, want to link the stream back to a path (cough cough GFA
     * loader cough cough)
     */
    template<typename Only>
    static tuple<unique_ptr<Only>> try_load_first_bare(istream& in, const string& filename, bool& found) {
        // Go try and get this thing
        auto loaded = try_load_bare<Only>(in, filename);
        if (loaded) {
            // We got this thing
            found = true;
        }
        
        return make_tuple(std::move(loaded));
    }
    
    /**
     * Allocate and load the first available type from the given iterator, using
     * a bare loader. Returns a tuple where at most one item is filled, and
     * that item is the first type we could successfully load out of the
     * template parameters. Sets found to true if anything is successfully loaded.
     */
    template<typename FirstPriority, typename SecondPriority, typename... Rest>
    static tuple<unique_ptr<FirstPriority>, unique_ptr<SecondPriority>, unique_ptr<Rest>...> try_load_first_encapsulated(MessageIterator& it, bool& found) {
    
        // We will load to here if we can
        tuple<unique_ptr<FirstPriority>> first_result;
        // And to here if we can't
        tuple<unique_ptr<SecondPriority>, unique_ptr<Rest>...> remaining_results;
        
        // Try and load the best thing. If we find a weird VPKG tag, don't skip over it.
        get<0>(first_result) = std::move(try_load_encapsulated<FirstPriority>(it));
        
        if (get<0>(first_result)) {
            // We found it
            found = true;
        } else {
            // It wasn't there.
            
            // Try the (nonempty) others
            remaining_results = std::move(try_load_first_encapsulated<SecondPriority, Rest...>(it, found));
        }
        
        return tuple_cat(std::move(first_result), std::move(remaining_results));
    }
    
    /**
     * Allocate and load the first available type from the given iterator, using
     * a VPKG-encapsulated loader. Returns a tuple where at most one item is
     * filled, and that item is the first type we could successfully load out
     * of the template parameters. Sets found to true if anything is successfully loaded.
     */
    template<typename Only>
    static tuple<unique_ptr<Only>> try_load_first_encapsulated(MessageIterator& it, bool& found) {
        // Go try and get this thing
        auto loaded = try_load_encapsulated<Only>(it);
        if (loaded) {
            // We got this thing
            found = true;
        }
        
        return make_tuple(std::move(loaded));
    }

    /**
     * Load an object of the given type from a stream.
     * The stream has to be a bare non-VPKG stream understood by the loader.
     * Returns the loaded object, or null if no loader liked the input.
     *
     * filename is optional, and can be used by callback code that may, for some reason,
     * want to link the stream back to a path (cough cough GFA loader cough cough).
     */
    template<typename Wanted>
    static unique_ptr<Wanted> try_load_bare(istream& in, const string& filename = "") {
        
        // We'll fill this in with the loaded object, if any.
        unique_ptr<Wanted> result;
        
        // Make sure we have putback capability.
        with_putback(in, [&](istream& from) {
            if(!from) {
                // We can't open the file; return an empty pointer.
                return;
            }
            
            // See if we can use a bare loader
            auto* bare_loaders = Registry::find_bare_loaders<Wanted>();
            
            if (bare_loaders != nullptr) {
                for (auto& loader_and_checker : *bare_loaders) {
                    // Just linear scan through all the loaders.
                    // Each checker must unget any characters it gets.
                    // Note that the checker may be null, in which case we skip
                    // the function because we wouldn't be able to tell the
                    // file from a type-tagged message file.
                    if (loader_and_checker.second != nullptr && loader_and_checker.second(from)) {
                        // Use the first loader that accepts this file.
                        // Up to the user to avoid prefix overlap.
                        result.reset((Wanted*)(loader_and_checker.first)(from, filename));
                        return;
                    }
                }
            }
        });
        
        return result;
    }
    
    /**
     * Load an object of the given type from a stream.
     * The stream has to be a VPKG type-tagged message stream.
     * Returns the loaded object, or null if the messages have incompatible
     * type tags. If the messages have incompatible tags, some may be consumed
     * from the stream.
     * Throws an exception if the input is not a VPKG type-tagged message stream.
     */
    template<typename Wanted>
    static unique_ptr<Wanted> try_load_encapsulated(istream& in) {

        unique_ptr<Wanted> result;
        
        with_putback(in, [&](istream& from) {
            
            if(!from) {
                // We can't open the file; return an empty pointer.
                return;
            }
            
            if (!BlockedGzipInputStream::SmellsLikeGzip(from)) {
                // This isn't compressed tagged data. It might be uncompressed
                // type-tagged data.
        
#ifdef debug
                cerr << "Sniffing tag from stream" << endl;
#endif
                
                // Sniff using get and unget.
                string sniffed_tag = MessageIterator::sniff_tag(from);
                
#ifdef debug
                cerr << "Sniffed tag: " << sniffed_tag << endl;
#endif
                if (sniffed_tag.empty()) {
                    // This isn't uncompressed tagged data either. So we can't load it.
                    return;
                }
                
                // See if we have a registered loader for this type.
                auto* loader = Registry::find_loader<Wanted>(sniffed_tag);
                
                if (!loader) {
                    // This has a tag we can't use and we know it, so stop now.
                    return;
                }
            }
            
            // If we get here, either it is GZIP-compressed (so we need to
            // really read some of it to start decompression), or it is
            // uncompressed and type-tagged with a tag we know.
            //
            // We want to proceed with making a MessageIterator and using its error
            // reporting to diagnose any problems with the file.
            MessageIterator it(from);
            
            // Try and load from the iterator
            result = std::move(try_load_encapsulated<Wanted>(it));
        });
        
        return result;
    }
    
    /**
     * Load an object of the given type from an iterator of type-tagged messages.
     * Returns the loaded object, or null if the messages have incompatible
     * type tags. If the messages have incompatible tags, the iterator will not be advanced.
     */
    template<typename Wanted>
    static unique_ptr<Wanted> try_load_encapsulated(MessageIterator& it) {

#ifdef debug
        cerr << "Iterator has a first item? " << it.has_current() << endl;
#endif
            
        if (it.has_current()) {
            // File is not empty
        
            string current_tag = (*it).first;
                
#ifdef debug
            cerr << "Iterator found tag \"" << current_tag << "\"" << endl;
#endif
            
            // See if we have one that has a registered loader for this type.
            auto* loader = Registry::find_loader<Wanted>(current_tag);
            
#ifdef debug
            cerr << "Loader for " << describe<Wanted>() << " from that tag: " << loader << endl;
#endif
            
            if (loader == nullptr) {
                // This isn't the right thing; Bail out and let someone else try again.
                return unique_ptr<Wanted>(nullptr);
            } else {
                // Load with it and return a unique_ptr for the result.
                return unique_ptr<Wanted>((Wanted*)(*loader)([&](const message_consumer_function_t& handle_message) {
                    while (it.has_current() && (*it).first == current_tag) {
                        // Feed in messages from the file until we run out or the tag changes
                        if ((*it).second.get() != nullptr) {
                            handle_message(*((*it).second));
                        }
                        ++it;
                    }
                }));
            }
        } else {
            // If the file is empty, return null.
            return unique_ptr<Wanted>(nullptr);
        }
    }

    /**
     * Run the given callback with a version of the given stream that allows putback.
     */
    static inline void with_putback(istream& in, const function<void(istream&)>& callback) {
        istream* from_ptr = &in;
        unique_ptr<streamistream> wrapper;
        if (&in == &cin) {
            // If reading from standard input, we buffer so magic number
            // sniffing can do putback.
            wrapper = make_unique<streamistream>(in);
            from_ptr = wrapper.get();
        }
        auto& from = *from_ptr;
        
        callback(from);
    }

    /**
     * Return a string to represent the given type. Should be demangled and human-readable.
     */
    template <typename T>
    static string describe() {
        // Get the (probably mangled) type name
        string mangled = typeid(T).name();
        
        // TODO: unify this demangling with crash.cpp
        int status;
            
        // Do the demangling
        char* demangledName = abi::__cxa_demangle(mangled.c_str(), NULL, NULL, &status);
        
        string demangled;
        if (status == 0) {
            // Demangling worked.
            // Wrap the char* in a string.
            demangled = string(demangledName);
        } else {
            // Demangling failed. Use mangled name.
            demangled = mangled;
        }
        
        if (demangledName != nullptr) {
            // Clean up the char*
            free(demangledName);
        }
        
        return demangled;
    }
};

}

}

#endif
