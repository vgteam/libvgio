#include "vg/io/alignment_io.hpp"
#include "vg/io/gafkluge.hpp"
#include "vg/io/edit.hpp"

#include <sstream>
#include <regex>
#include <cmath>

//#define debug_translation

namespace vg {

namespace io {

bool get_next_record_from_gaf(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, htsFile* fp, kstring_t& s_buffer, gafkluge::GafRecord& record) {
    
    if (hts_getline(fp, '\n', &s_buffer) <= 0) {
        return false;
    }

    gafkluge::parse_gaf_record(ks_str(&s_buffer), record);
    return true;
}

bool get_next_interleaved_record_pair_from_gaf(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, htsFile* fp, kstring_t& s_buffer,
                                               gafkluge::GafRecord& record1, gafkluge::GafRecord& record2) {
    return get_next_record_from_gaf(node_to_length, node_to_sequence, fp, s_buffer, record1) &&
        get_next_record_from_gaf(node_to_length, node_to_sequence, fp, s_buffer, record2);
}

size_t gaf_unpaired_for_each(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename, function<void(Alignment&)> lambda) {

    htsFile* in = hts_open(filename.c_str(), "r");
    if (in == NULL) {
        cerr << "[vg::alignment.cpp] couldn't open " << filename << endl; exit(1);
    }
    
    kstring_t s_buffer = KS_INITIALIZE;
    Alignment aln;
    gafkluge::GafRecord gaf;
    size_t count = 0;

    while (get_next_record_from_gaf(node_to_length, node_to_sequence, in, s_buffer, gaf) == true) {
        gaf_to_alignment(node_to_length, node_to_sequence, gaf, aln);
        lambda(aln);
        ++count;
    }
    
    hts_close(in);

    return count;
}

size_t gaf_unpaired_for_each(const HandleGraph& graph, const string& filename, function<void(Alignment&)> lambda) {
    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    return gaf_unpaired_for_each(node_to_length, node_to_sequence, filename, lambda);
}

size_t gaf_paired_interleaved_for_each(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                       function<void(Alignment&, Alignment&)> lambda) {

    htsFile* in = hts_open(filename.c_str(), "r");
    if (in == NULL) {
        cerr << "[vg::alignment.cpp] couldn't open " << filename << endl; exit(1);
    }
    
    kstring_t s_buffer = KS_INITIALIZE;
    Alignment aln1, aln2;
    gafkluge::GafRecord gaf1, gaf2;
    size_t count = 0;

    while (get_next_interleaved_record_pair_from_gaf(node_to_length, node_to_sequence, in, s_buffer, gaf1, gaf2) == true) {
        gaf_to_alignment(node_to_length, node_to_sequence, gaf1, aln1);
        gaf_to_alignment(node_to_length, node_to_sequence, gaf2, aln2);
        lambda(aln1, aln2);
        count += 2;
    }
    
    hts_close(in);

    return count;
}

size_t gaf_paired_interleaved_for_each(const HandleGraph& graph, const string& filename,
                                       function<void(Alignment&, Alignment&)> lambda) {
    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    return gaf_paired_interleaved_for_each(node_to_length, node_to_sequence, filename, lambda);
}

size_t gaf_unpaired_for_each_parallel(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                      function<void(Alignment&)> lambda,
                                      uint64_t batch_size) {

    htsFile* in = hts_open(filename.c_str(), "r");
    if (in == NULL) {
        cerr << "[vg::alignment.cpp] couldn't open " << filename << endl; exit(1);
    }

    kstring_t s_buffer = KS_INITIALIZE;
    
    function<bool(gafkluge::GafRecord&)> get_read = [&](gafkluge::GafRecord& gaf) {
        return get_next_record_from_gaf(node_to_length, node_to_sequence, in, s_buffer, gaf);
    };

    function<void(gafkluge::GafRecord&)> gaf_lambda = [&] (gafkluge::GafRecord& gaf) {
        Alignment aln;
        gaf_to_alignment(node_to_length, node_to_sequence, gaf, aln);
        lambda(aln);
    };
        
    size_t nLines = unpaired_for_each_parallel(get_read, gaf_lambda, batch_size);
    
    hts_close(in);
    return nLines;

}

size_t gaf_unpaired_for_each_parallel(const HandleGraph& graph, const string& filename,
                                      function<void(Alignment&)> lambda,
                                      uint64_t batch_size) {    
    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    return gaf_unpaired_for_each_parallel(node_to_length, node_to_sequence, filename, lambda, batch_size);
}

size_t gaf_paired_interleaved_for_each_parallel(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                                function<void(Alignment&, Alignment&)> lambda,
                                                uint64_t batch_size) {
    return gaf_paired_interleaved_for_each_parallel_after_wait(node_to_length, node_to_sequence, filename, lambda, [](void) {return true;}, batch_size);
}

size_t gaf_paired_interleaved_for_each_parallel(const HandleGraph& graph, const string& filename,
                                                function<void(Alignment&, Alignment&)> lambda,
                                                uint64_t batch_size) {
    return gaf_paired_interleaved_for_each_parallel_after_wait(graph, filename, lambda, [](void) {return true;}, batch_size);
}

size_t gaf_paired_interleaved_for_each_parallel_after_wait(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                                           function<void(Alignment&, Alignment&)> lambda,
                                                           function<bool(void)> single_threaded_until_true,
                                                           uint64_t batch_size) {
    
    htsFile* in = hts_open(filename.c_str(), "r");
    if (in == NULL) {
        cerr << "[vg::alignment.cpp] couldn't open " << filename << endl; exit(1);
    }

    kstring_t s_buffer = KS_INITIALIZE;
    
    function<bool(gafkluge::GafRecord&, gafkluge::GafRecord&)> get_pair = [&](gafkluge::GafRecord& mate1, gafkluge::GafRecord& mate2) {
        return get_next_interleaved_record_pair_from_gaf(node_to_length, node_to_sequence, in, s_buffer, mate1, mate2);
    };

    function<void(gafkluge::GafRecord&, gafkluge::GafRecord&)> gaf_lambda = [&] (gafkluge::GafRecord& mate1, gafkluge::GafRecord& mate2) {
        Alignment aln1, aln2;
        gaf_to_alignment(node_to_length, node_to_sequence, mate1, aln1);
        gaf_to_alignment(node_to_length, node_to_sequence, mate2, aln2);
        lambda(aln1, aln2);        
    };
    size_t nLines = paired_for_each_parallel_after_wait(get_pair, gaf_lambda, single_threaded_until_true, batch_size);

    hts_close(in);
    return nLines;    
}

size_t gaf_paired_interleaved_for_each_parallel_after_wait(const HandleGraph& graph, const string& filename,
                                                           function<void(Alignment&, Alignment&)> lambda,
                                                           function<bool(void)> single_threaded_until_true,
                                                           uint64_t batch_size) {
    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    return gaf_paired_interleaved_for_each_parallel_after_wait(node_to_length, node_to_sequence, filename, lambda, single_threaded_until_true, batch_size);
}

gafkluge::GafRecord alignment_to_gaf(function<size_t(nid_t)> node_to_length,
                                     function<string(nid_t, bool)> node_to_sequence,
                                     const Alignment& aln,
                                     const handlegraph::NamedNodeBackTranslation* translate_through,
                                     bool cs_cigar,
                                     bool base_quals,
                                     bool frag_links) {

    // TODO: We can't support translations with alignments that end up split
    // (arriving to or leaving from the middle of a segment) in segment space,
    // even if they weren't split in node space, but we also can't *detect*
    // them, because we don't have a way to get lengths and sequences of entire
    // segments, because NamedNodeBackTranslation doesn't provide functions
    // that go the right direction. So results will be wrong if such alignments
    // are provided!
    // Don't use translation with graphs where segments have been anything but
    // straightforwardly chopped, or where any alignments are split/can jump!

    gafkluge::GafRecord gaf;

    //1 string Query sequence name
    gaf.query_name = aln.name();

    //2 int Query sequence length
    gaf.query_length = aln.sequence().length();

    //12 int Mapping quality (0-255; 255 for missing)
    //Note: protobuf can't distinguish between 0 and missing so we just copy it through
    gaf.mapq = aln.mapping_quality();

    // we add these annotations early so that if necessary they can be overwritten by the
    // more directly purposeful GAF annotations later in this function
    // TODO: ugly that we have to replicate parts of vg's nice annotation.hpp system here...
    const auto& annotations = aln.annotation();
    if (annotations.fields().count("tags")) {
        vector<string> tokens;
        string tag_string = annotations.fields().at("tags").string_value();
        size_t i = 0;
        while (i < tag_string.size() && isspace(tag_string[i])) {
            ++i;
        }
        while (i < tag_string.size()) {
            size_t j = tag_string.find_first_of(" \t\n\r\f\v", i + 1);
            if (j > i + 1) {
                tokens.emplace_back(tag_string.substr(i, j - i));
            }
            i = j;
            while (i < tag_string.size() && isspace(tag_string[i])) {
                ++i;
            }
        }
        
        for (const auto& tag : tokens) {
            if (tag.size() < 6 || tag[2] != ':' || tag[4] != ':') {
                cerr << "error: invalid SAM-style tag annotation: " << tag << '\n';
                exit(1);
            }
            // split into values
            gaf.opt_fields[tag.substr(0, 2)] = make_pair(tag.substr(3, 1), tag.substr(5, string::npos));
        }
    }
    
    if (aln.has_path() && aln.path().mapping_size() > 0) {    
        //3 int Query start (0-based; closed)
        gaf.query_start = 0; //(aln.path().mapping_size() ? first_path_position(aln.path()).offset() : "*") << "\t"
        //4 int Query end (0-based; open)
        gaf.query_end = aln.sequence().length();
        //5 char Strand relative to the path: "+" or "-"
        gaf.strand = '+'; // always positive relative to the path
        //7 int Path length
        gaf.path_length = 0;
        //8 int Start position on the path (0-based)
        gaf.path_start = gafkluge::missing_int;
        //10 int Number of residue matches
        gaf.matches = 0;
        gaf.path.reserve(aln.path().mapping_size());
        string cs_cigar_str;
        size_t running_match_length = 0;
        // Track running deletion status for CIGAR string.
        // if set, can just print bases (without "-") to continue
        bool running_deletion = false;

        // We may have output a softclip at the start of a new node as the final mapping.
        // In that case, we do not output the node in the path and we need to trigger the
        // final mapping logic one step earlier.
        size_t final_mapping = aln.path().mapping_size() - 1;
        if (final_mapping > 0 &&
            aln.path().mapping(final_mapping).edit_size() == 1 &&
            edit_is_insertion(aln.path().mapping(final_mapping).edit(0))) {
            final_mapping--;
        }

        size_t total_to_len = 0;
        size_t prev_offset;
        handlegraph::oriented_node_range_t prev_range;
        for (size_t mapping_index = 0; mapping_index < aln.path().mapping_size(); ++mapping_index) {
            auto& mapping = aln.path().mapping(mapping_index);
            const Position& position = mapping.position();
            // This stores the offset along the current node that we arrived at. 
            size_t start_offset_on_node = position.offset();
            // This is our offset along the graph node, and is advanced as a
            // cursor as we look at the edits.
            size_t offset = start_offset_on_node;
            // This is our difference from node offset to segment offset, if applicable
            size_t node_to_segment_offset = 0;
            size_t node_length = node_to_length(position.node_id());
            string node_seq;
            bool skip_step = false;

#ifdef debug_translation
            std::cerr << "At node " << position.node_id() << " and starting at offset " << offset << std::endl;
#endif

            if (cs_cigar == true && mapping_index > 0 && start_offset_on_node > 0) {
                // We need to do something in the CIGAR about how we skipped the start of a node we arrived at.
                if (start_offset_on_node == prev_offset && position.node_id() == aln.path().mapping(mapping_index -1).position().node_id() &&
                    position.is_reverse() == aln.path().mapping(mapping_index -1).position().is_reverse()) {
                    // our mapping is redundant, we won't write a step for it
                    skip_step = true;
                } else {
                    // to support split-mappings we gobble up the beginnings
                    // of nodes using deletions since unlike GAM, we can only
                    // set the offset of the first node
                    if (translate_through) {
                        // We can't do this if we don't have a way to get segment lengths, and that's not in the interface yet.
                        throw std::runtime_error("Split alignments cannot be converted to named-segment-space GAF");
                    }
                    if (node_seq.empty()) {
                        node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                    }
                    // vg's chunked mapper will happily add new Mappings on the same node
                    // so we try to keep that in mind here where we subtract out the previous offset
                    // before deleting to not double count
                    int64_t del_start_offset = 0;
                    if (position.node_id() == aln.path().mapping(mapping_index - 1).position().node_id()) {
                        del_start_offset = prev_offset;
                    }
                    if (start_offset_on_node > del_start_offset) {
                        if (running_match_length > 0) {
                            // Matches are : followed by the match length
                            cs_cigar_str += ":" + std::to_string(running_match_length);
                            running_match_length = 0;
                        }
                        if (!running_deletion) {
                            cs_cigar_str += "-";
                        }
                        cs_cigar_str += node_seq.substr(del_start_offset, start_offset_on_node - del_start_offset);
                        running_deletion = true;
                    }
                }
            }
            
            for (size_t j = 0; j < mapping.edit_size(); ++j) {
                // Scan the edits and work out how much we span on the node.
#ifdef debug_translation
                std::cerr << "At edit " << j << std::endl;
#endif
                auto& edit = mapping.edit(j);
                if (edit_is_match(edit)) {
                    // Count the matches towards the GAF's total match count
                    gaf.matches += edit.from_length();
                }
                if (cs_cigar == true) {
                    // CS-cigar string
                    if (edit_is_match(edit)) {
                        // Merge up matches that span edits/mappings
                        running_match_length += edit.from_length();
                        running_deletion = false;
                    } else {
                        if (running_match_length > 0) {
                            // Matches are : followed by the match length
                            cs_cigar_str += ":" + std::to_string(running_match_length);
                            running_match_length = 0;                            
                        }
                        if (edit_is_sub(edit)) {
                            if (node_seq.empty()) {
                                node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                            }
                            // Substitions expressed one base at a time, preceded by *
                            for (size_t k = 0; k < edit.from_length(); ++k) {
                                cs_cigar_str += "*" + node_seq.substr(offset + k, 1) + edit.sequence().substr(k, 1); 
                            }
                            running_deletion = false;
                        } else if (edit_is_deletion(edit)) {
                            if (node_seq.empty()) {
                                node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                            }
                            // Deletion is - followed by deleted sequence
                            assert(offset + edit.from_length() <= node_seq.length());
                            if (!running_deletion) {
                                cs_cigar_str += "-";
                            }
                            cs_cigar_str += node_seq.substr(offset, edit.from_length());
                            running_deletion = true;
                        } else if (edit_is_insertion(edit)) {
                            // Insertion is "+" followed by inserted sequence
                            cs_cigar_str += "+" + edit.sequence();
                            running_deletion = false;
                        }
                    }
                }
                offset += edit.from_length();
                total_to_len += edit.to_length();
#ifdef debug_translation
                std::cerr << "Move offset by edit from length of " << edit.from_length() << " to " << offset << std::endl;
#endif
            }

            // Determine the range on a node we are aligned against
            handlegraph::oriented_node_range_t range(position.node_id(), position.is_reverse(),
                                                     start_offset_on_node, offset - start_offset_on_node);

            if (translate_through) {
                // We need to articulate this step on the path
                // back-translated to segment name space, and skip
                // duplicate segment names that aren't going around
                // self-loops.
                
                // Translate it
                std::vector<handlegraph::oriented_node_range_t> translated = translate_through->translate_back(range);
                
                if (translated.size() != 1) {
                    // TODO: Implement translations that split graph nodes into multiple segments.
                    throw std::runtime_error("Translated range on node " + std::to_string(std::get<0>(range)) +
                                             " to " + std::to_string(translated.size()) + 
                                             " named segment ranges, but complex translations like this are not yet implemented");
                }
                
                auto& translated_range = translated[0];
                if (std::get<1>(translated_range) != std::get<1>(range)) {
                    // TODO: Implement translations that flip orientations.
                    throw std::runtime_error("Translated range on node " + std::to_string(std::get<0>(range)) +
                                             " ended up on the opposite strand; complex translations like this are not yet implemented");
                }

                // Record how far ahead the node is of the segment
                node_to_segment_offset = get<2>(translated_range) - get<2>(range);

#ifdef debug_translation
                std::cerr << "Translated range node:" << get<0>(range) << " orientation:" << get<1>(range) << " start:" << get<2>(range) << " length:" << get<3>(range)
                    << " to segment:" << get<0>(translated_range) << " orientation:" << get<1>(translated_range) << " start:" << get<2>(translated_range) << " length:" << get<3>(translated_range) 
                    << " leaving node to segment offset of " << node_to_segment_offset << std::endl;
#endif

                // Commit back the translation
                range = translated_range;
            }

            if (mapping_index == 0) {
                // use path_start to store the offset of the first node
                gaf.path_start = std::get<2>(range);
                
#ifdef debug_translation
                std::cerr << "This is the first mapping so we start the alignment at " << gaf.path_start << " on path" << std::endl;
#endif
            } else if (mapping_index > final_mapping) {
                // this is another case that comes up, giraffe adds an empty mapping for a softclip at the
                // end.  there's no real way for the GAF cigar to distinguish these, so make sure it doesn't come up
#ifdef debug_translation
                std::cerr << "This is a final softclip-only mapping, so hide it." << gaf.path_start << std::endl;
#endif
                skip_step = true;
            }

            if (mapping_index < aln.path().mapping_size() - 1 && offset != node_length) {
                // We aren't the last mapping but we are ending before our node is done
                if (position.node_id() != aln.path().mapping(mapping_index + 1).position().node_id() ||
                    position.is_reverse() != aln.path().mapping(mapping_index + 1).position().is_reverse()) {
                    // we are hopping off the middle of a node, need to gobble it up with a deletion
#ifdef debug_translation
                    std::cerr << "Alignment leaves before reaching end of node" << std::endl;
#endif
                    if (translate_through) {
                        // We can't do this if we don't have a way to get segment lengths, and that's not in the interface yet.
                        throw std::runtime_error("Split alignments cannot be converted to named-segment-space GAF");
                    }
                    if (node_seq.empty()) {
                        node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                    }
                    if (running_match_length > 0) {
                        // Matches are : followed by the match length
                        cs_cigar_str += ":" + std::to_string(running_match_length);
                        running_match_length = 0;
                    }
                    if (!running_deletion) {
                        cs_cigar_str += "-";
                    } 
                    cs_cigar_str += node_seq.substr(offset);
                    running_deletion = true;
                } else {
                    // we have a duplicate node mapping.  vg map actually produces these sometimes
                    // where an insert gets its own mapping even though its from_length is 0
                    // the gaf cigar format assumes nodes are fully covered, so we squish it out.
#ifdef debug_translation
                    std::cerr << "Alignment has another mapping to this node after this one, so skip this one" << std::endl;
#endif
                    skip_step = true;
                }
            }
            
            //6 string Path matching /([><][^\s><]+(:\d+-\d+)?)+|([^\s><]+)/
            if (!skip_step) {
                gaf.path_length += node_length;
#ifdef debug_translation
                std::cerr << "Add full node length of " << node_length << " to GAF path" << std::endl;
#endif
                
                // Now we consult range for things like the offset
                if (mapping_index == 0) {
                    // Update the stored path start.
                    // TODO: didn't we do this already?
                    gaf.path_start = std::get<2>(range);
#ifdef debug_translation
                    std::cerr << "This is the first mapping so start alignment at " << std::get<2>(range) << " on path (again)" << std::endl;
#endif
                    // Account for any part of the path in the segment before our first node
                    gaf.path_length += node_to_segment_offset;
#ifdef debug_translation
                    std::cerr << "This is the first mapping so also add unused prefix of its segment (" << node_to_segment_offset << ")" << std::endl;
#endif
                } else if (translate_through) {
                    // We need to filter out consecutive visits to pieces of
                    // the same segment that abut each other, so we don't get
                    // the segment named multiple times. We keep pieces that
                    // don't abut each other but look like going around a self
                    // loop, and we bail out (for now) on pieces that
                    // arbitrarily jump around the segment, since we don't know
                    // how to synthesize the deletions.
                    // TODO: jumps ahead could be synthesized into deletions relatively easily.
                    if (std::get<0>(range) == std::get<0>(prev_range) &&
                        std::get<1>(range) == std::get<1>(prev_range)) {
                        // We're on the same segment and orientation as the last mapping
#ifdef debug_translation
                        std::cerr << "We're on the same segment and orientation as the previous mapping" << std::endl;
#endif
                        if (std::get<2>(range) == std::get<2>(prev_range) + std::get<3>(prev_range)) {
                            // And we abut perfectly; nothing has been skipped over.
                            // We don't need to report the segment again in the GAF path.
#ifdef debug_translation
                            std::cerr << "And we abut perfectly, so skip the step." << std::endl;
#endif
                            skip_step = true;
                        } else if (std::get<2>(range) != 0) {
                            // We're arriving at the same segment at somewhere
                            // other than the start. This is definitely a split
                            // alignment in segment space! We can't handle
                            // those yet!
                            throw std::runtime_error("Alignments that become split in segment space cannot be converted to named-segment-space GAF");
                        } else {
                            // Otherwise we're arriving at the start of the same
                            // segment. Still might be a split alignment that we
                            // forbid, but if not we do want to report the same
                            // segment again because we go through it again.
#ifdef debug_translation
                            std::cerr << "We're repeating the segment" << std::endl;
#endif
                        }
                    }
                }
                
                if (!skip_step) {
                    // Actually report this visit to this node or segment.
                    gafkluge::GafStep step;
                    step.name = translate_through ? translate_through->get_back_graph_node_name(std::get<0>(range)) : std::to_string(std::get<0>(range));
                    step.is_stable = false;
                    step.is_reverse = std::get<1>(range);
                    step.is_interval = false;
                    
                    gaf.path.push_back(std::move(step));
#ifdef debug_translation
                    std::cerr << "Added step to GAF path" << std::endl;
#endif
                }
            }
            
            if (mapping_index == final_mapping) {
#ifdef debug_translation
                std::cerr << "We are the final mapping" << std::endl;
#endif

                //9 int End position on the path (0-based)

#ifdef debug_translation
                std::cerr << "Path is overall " << gaf.path_length << std::endl;
#endif

                assert(!gafkluge::is_missing(gaf.path_start));

                // Right now the path length includes all the stuff on the
                // start segment before we actually pick up, and all the
                // graph nodes we visit (even if we don't use the full
                // node), but not any of the last segment after the last
                // node we use.
                size_t unused_node = node_length - offset;
                gaf.path_end = gaf.path_length - unused_node;
#ifdef debug_translation
                std::cerr << "There are " << unused_node << " bases on the last node in the path not used by the alignment, so the alignment ends at " << gaf.path_end << std::endl;
#endif

                if (translate_through) {
                    // But we also have to account in the path length for the part
                    // of the segment that comes after the node we stop at. So
                    // translate offset 0 on its reverse strand to measure the
                    // offset from there to the segment end.
                    handlegraph::oriented_node_range_t stop_pos_rev_strand(position.node_id(), !position.is_reverse(),
                                                                           0, 0);
                    
                
                    // Translate it
                    std::vector<handlegraph::oriented_node_range_t> translated = translate_through->translate_back(stop_pos_rev_strand);
                    // Add any offset to the path length.
                    gaf.path_length += std::get<2>(translated.at(0));
#ifdef debug_translation
                    std::cerr << "Path is actually " << gaf.path_length << " because we need to add the remaining " << std::get<2>(translated.at(0)) << " bases on the segment after the node" << std::endl;
#endif
                }
            }

            prev_range = range;
            prev_offset = offset;
        }
        if (cs_cigar && running_match_length > 0) {
            cs_cigar_str += ":" + std::to_string(running_match_length);
            running_match_length = 0;
        }

        // We can support gam alignments without sequences by inferring the sequence length from edits
        if (gaf.query_length == 0 && total_to_len > 0) {
            gaf.query_length = total_to_len;
            gaf.query_end = total_to_len;
        } 

        //11 int Alignment "block length"
        // This is actually longest sequence in the alignment.
        gaf.block_length = std::max(gaf.path_end - gaf.path_start, gaf.query_length);

        // optional cs-cigar string
        if (cs_cigar) {
            gaf.opt_fields["cs"] = make_pair("Z", std::move(cs_cigar_str));
        }

        // convert the identity into the dv divergence field
        // https://lh3.github.io/minimap2/minimap2.html#10
        if (aln.identity() > 0) {
            stringstream dv_str;
            dv_str << std::floor((1. - aln.identity()) * 10000. + 0.5) / 10000.;
            gaf.opt_fields["dv"] = make_pair("f", dv_str.str());
        }

        // convert the score into the AS field
        // https://lh3.github.io/minimap2/minimap2.html#10
        if (aln.score() > 0) {
            gaf.opt_fields["AS"] = make_pair("i", std::to_string(aln.score()));
        }

        // optional base qualities
        if (base_quals && !aln.quality().empty()) { 
            gaf.opt_fields["bq"] = make_pair("Z", string_quality_short_to_char(aln.quality()));
        }

        if (aln.has_annotation()) {
            auto& annotation = aln.annotation();
            if (annotation.fields().count("proper_pair")) {
                bool is_properly_paired = (annotation.fields().at("proper_pair")).bool_value();
                gaf.opt_fields["pd"] = make_pair("b", is_properly_paired ? "1" : "0");
            }
            if (annotation.fields().count("support")) {
                gaf.opt_fields["AD"] = make_pair("i", (annotation.fields().at("support")).string_value());
            }
        }
    }

    // optional frag_next/prev names
    if (frag_links == true) {
      if (aln.has_fragment_next()) {
        gaf.opt_fields["fn"] = make_pair("Z", aln.fragment_next().name());
      }
      if (aln.has_fragment_prev()) {
        gaf.opt_fields["fp"] = make_pair("Z", aln.fragment_prev().name());
      }
    }

    return gaf;    
}

gafkluge::GafRecord alignment_to_gaf(const HandleGraph& graph,
                                     const Alignment& aln,
                                     const handlegraph::NamedNodeBackTranslation* translate_through,
                                     bool cs_cigar,
                                     bool base_quals,
                                     bool frag_links) {

    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    return alignment_to_gaf(node_to_length, node_to_sequence, aln, translate_through, cs_cigar, base_quals, frag_links);
}

void gaf_to_alignment(function<size_t(nid_t)> node_to_length,
                      function<string(nid_t, bool)> node_to_sequence,
                      const gafkluge::GafRecord& gaf,
                      Alignment& aln){

    aln.Clear();

    if (!gafkluge::is_missing(gaf.query_name)) {
        aln.set_name(gaf.query_name);
    }

    for (size_t i = 0; i < gaf.path.size(); ++i) {
        const auto& gaf_step = gaf.path[i];
        // only support unstable gaf at this point
        assert(gaf_step.is_stable == false);
        assert(gaf_step.is_interval == false);
        Mapping* mapping = aln.mutable_path()->add_mapping();
        mapping->mutable_position()->set_node_id(std::stol(gaf_step.name));
        mapping->mutable_position()->set_is_reverse(gaf_step.is_reverse);
        if (i == 0) {
            mapping->mutable_position()->set_offset(gaf.path_start);
        }
        mapping->set_rank(i + 1);
    }

    if (gaf.mapq != 255) {
        // We let 255 be equivalent to 0, which isn't great
        aln.set_mapping_quality(gaf.mapq);
    }

    if (!gaf.path.empty()) {
        size_t cur_mapping = 0;
        int64_t cur_offset = gaf.path_start;
        Position cur_position = aln.path().mapping(cur_mapping).position();
        size_t cur_len = node_to_length(cur_position.node_id());
        string& sequence = *aln.mutable_sequence();
        bool from_cg = false;
        // Use the CS cigar string to add Edits into our Path, as well as set the sequence
        gafkluge::for_each_cigar(gaf, [&] (const char& cigar_cat, const size_t& cigar_len, const string& cigar_query, const string& cigar_target) {
                assert(cur_offset < cur_len || ((cigar_cat == '+' || cigar_cat == 'I' || cigar_cat == 'S') && cur_offset <= cur_len));
                if (!from_cg && cigar_cat != ':' && cigar_cat != '+' && cigar_cat != '-' && cigar_cat != '*') {
                    from_cg = true;
                }

                if (cigar_cat == ':' || cigar_cat == 'M' || cigar_cat == '=' || cigar_cat == 'X') {
                    int64_t match_len = (int64_t)cigar_len;
                    while (match_len > 0) {
                        int64_t current_match = std::min(match_len, (int64_t)node_to_length(cur_position.node_id()) - cur_offset);
                        Edit* edit = aln.mutable_path()->mutable_mapping(cur_mapping)->add_edit();
                        edit->set_from_length(current_match);
                        edit->set_to_length(current_match);
                        if (cigar_cat == 'X') {
                            // add a phony snp
                            edit->set_sequence(string(current_match, 'N'));
                        }
                        if (node_to_sequence) {
                            sequence += node_to_sequence(cur_position.node_id(), cur_position.is_reverse()).substr(cur_offset, current_match);
                        }
                        match_len -= current_match;
                        cur_offset += current_match;
                        if (match_len > 0) {
                            assert(cur_mapping < aln.path().mapping_size() - 1);
                            ++cur_mapping;
                            cur_offset = 0;
                            cur_position = aln.path().mapping(cur_mapping).position();
                            cur_len = node_to_length(cur_position.node_id());
                        }
                    }
                } else if (cigar_cat == '+' || cigar_cat == 'I' || cigar_cat == 'S') {
                    size_t tgt_mapping = cur_mapping;
                    // left-align insertions to try to be more consistent with vg
                    if (cur_offset == 0 && cur_mapping > 0 && (!aln.path().mapping(cur_mapping - 1).position().is_reverse()
                                                               || cur_mapping == aln.path().mapping_size())) {
                        --tgt_mapping;
                    }
                    Edit* edit = aln.mutable_path()->mutable_mapping(tgt_mapping)->add_edit();
                    edit->set_from_length(0);
                    edit->set_to_length(cigar_len);
                    if (cigar_cat == '+') {
                        edit->set_sequence(cigar_query);
                    } else {
                        // todo: better to leave this empty?  I think client code may be expecting sequence so we give it
                        edit->set_sequence(string(cigar_len, 'N'));
                    }
                    sequence += edit->sequence();
                } else if (cigar_cat == '-' || cigar_cat == 'D') {
                    int64_t del_len = (int64_t)cigar_len;
                    while (del_len > 0) {
                        int64_t current_del = std::min(del_len, (int64_t)node_to_length(cur_position.node_id()) - cur_offset);
                        Edit* edit = aln.mutable_path()->mutable_mapping(cur_mapping)->add_edit();
                        edit->set_to_length(0);
                        edit->set_from_length(current_del);
                        del_len -= current_del;
                        cur_offset += current_del;
                        // like matches, we allow deletions to span multiple nodes now.
                        if (del_len > 0) {
                            assert(cur_mapping < aln.path().mapping_size() - 1);
                            ++cur_mapping;
                            cur_offset = 0;
                            cur_position = aln.path().mapping(cur_mapping).position();
                            cur_len = node_to_length(cur_position.node_id());
                        }
                    }
                } else if (cigar_cat == '*') {
                    assert(cigar_len == 1);
                    assert(!node_to_sequence || node_to_sequence(cur_position.node_id(), cur_position.is_reverse()).substr(cur_offset,1) == cigar_target);
                    Edit* edit = aln.mutable_path()->mutable_mapping(cur_mapping)->add_edit();
                    // todo: support multibase snps
                    edit->set_from_length(cigar_len);
                    edit->set_to_length(cigar_len);
                    edit->set_sequence(cigar_query);
                    sequence += edit->sequence();
                    ++cur_offset;
                } else {
                    //todo: better error (warning?)
                    assert(false);
                }
            
                // advance to the next mapping if we've pushed the offset past the current node
                assert(cur_offset <= cur_len);
                if (cur_offset == cur_len) {
                    ++cur_mapping;
                    cur_offset = 0;
                    if (cur_mapping < aln.path().mapping_size()) {
                        cur_position = aln.path().mapping(cur_mapping).position();
                        cur_len = node_to_length(cur_position.node_id());
                    }
                }
            });

        // this is to support gafs that were made from alignments where the last mapping is
        // nothing but a soft clip:
        // https://github.com/vgteam/vg/issues/3533
        // note these cases will be prevented going forward in fix to alignment_to_gaf() to stop
        // ambiguous gafs from being written in the first place
        size_t num_mappings = aln.path().mapping_size();
        if (num_mappings > 1 && aln.path().mapping(num_mappings - 1).edit_size() == 0 &&
            aln.path().mapping(num_mappings - 2).edit_size() > 0 &&
            edit_is_insertion(aln.path().mapping(num_mappings - 2).edit(aln.path().mapping(num_mappings - 2).edit_size() - 1))) {
            Path path_cpy = aln.path();
            aln.clear_path();
            for (size_t i = 0; i < num_mappings - 1; ++i) {
                *aln.mutable_path()->add_mapping() = path_cpy.mapping(i);
            }
        }
        
        if (from_cg) {
            // remember that we came from a lossy cg-cigar -> GAM conversion path
            auto* annotation = aln.mutable_annotation();
            google::protobuf::Value is_from_cg;
            is_from_cg.set_bool_value(from_cg);
            (*annotation->mutable_fields())["from_cg"] = is_from_cg;
        }
    }

    for (auto opt_it : gaf.opt_fields) {
        if (opt_it.first == "dv") {
            // get the identity from the dv divergence field
            // https://lh3.github.io/minimap2/minimap2.html#10
            aln.set_identity(1. - std::stof(opt_it.second.second));
        } else if (opt_it.first == "AS") {
            // get the score from the AS field
            // https://lh3.github.io/minimap2/minimap2.html#10
            aln.set_score(std::stoi(opt_it.second.second));
        } else if (opt_it.first == "bq") {
            // get the quality from the bq field
            aln.set_quality(string_quality_char_to_short(opt_it.second.second));
        } else if (opt_it.first == "fp") {
            // get the fragment_previous field
            aln.mutable_fragment_prev()->set_name(opt_it.second.second);
        } else if (opt_it.first == "fn") {
            // get the fragment_next field
            aln.mutable_fragment_next()->set_name(opt_it.second.second);
        } else if (opt_it.first == "pd") {
            //Is this read properly paired
            auto* annotation = aln.mutable_annotation();
            google::protobuf::Value is_properly_paired;
            is_properly_paired.set_bool_value(opt_it.second.second == "1");
            (*annotation->mutable_fields())["proper_pair"] = is_properly_paired;
        }
    }
}

void gaf_to_alignment(const HandleGraph& graph,
                      const gafkluge::GafRecord& gaf,
                      Alignment& aln) {

    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    gaf_to_alignment(node_to_length, node_to_sequence, gaf, aln); 
}

short quality_char_to_short(char c) {
    return static_cast<short>(c) - 33;
}

char quality_short_to_char(short i) {
    return static_cast<char>(i + 33);
}

void alignment_quality_short_to_char(Alignment& alignment) {
    alignment.set_quality(string_quality_short_to_char(alignment.quality()));
}

string string_quality_short_to_char(const string& quality) {
    string buffer; buffer.resize(quality.size());
    for (int i = 0; i < quality.size(); ++i) {
        buffer[i] = quality_short_to_char(quality[i]);
    }
    return buffer;
}

void alignment_quality_char_to_short(Alignment& alignment) {
    alignment.set_quality(string_quality_char_to_short(alignment.quality()));
}

string string_quality_char_to_short(const string& quality) {
    string buffer; buffer.resize(quality.size());
    for (int i = 0; i < quality.size(); ++i) {
        buffer[i] = quality_char_to_short(quality[i]);
    }
    return buffer;
}


}
}
