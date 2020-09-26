#include "vg/io/alignment_io.hpp"
#include "vg/io/gafkluge.hpp"
#include "vg/io/edit.hpp"

#include <sstream>
#include <regex>
#include <cmath>

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

gafkluge::GafRecord alignment_to_gaf(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const Alignment& aln, bool cs_cigar, bool base_quals) {

    gafkluge::GafRecord gaf;

    //1 string Query sequence name
    gaf.query_name = aln.name();

    //2 int Query sequence length
    gaf.query_length = aln.sequence().length();

    //12 int Mapping quality (0-255; 255 for missing)
    //Note: protobuf can't distinguish between 0 and missing so we just copy it through
    gaf.mapq = aln.mapping_quality();

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
        gaf.path_start = 0;
        //10 int Number of residue matches
        gaf.matches = 0;
        gaf.path.reserve(aln.path().mapping_size());
        string cs_cigar_str;
        size_t running_match_length = 0;
        size_t total_to_len = 0;
        size_t prev_offset;
        for (size_t i = 0; i < aln.path().mapping_size(); ++i) {
            auto& mapping = aln.path().mapping(i);
            size_t offset = mapping.position().offset();
            string node_seq;
            const Position& position = mapping.position();
            bool skip_step = false;
            if (i == 0) {
                // use path_start to store the offset of the first node
                gaf.path_start = offset;
            } else if (cs_cigar == true && offset > 0) {
                if (offset == prev_offset && mapping.position().node_id() == aln.path().mapping(i -1).position().node_id() &&
                    mapping.position().is_reverse() == aln.path().mapping(i -1).position().is_reverse()) {
                    // our mapping is redundant, we won't write a step for it
                    skip_step = true;
                } else {
                    // to support split-mappings we gobble up the beginnings
                    // of nodes using deletions since unlike GAM, we can only
                    // set the offset of the first node
                    if (node_seq.empty()) {
                        node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                    }
                    cs_cigar_str += "-" + node_seq.substr(0, offset);
                }
            }
            for (size_t j = 0; j < mapping.edit_size(); ++j) {
                auto& edit = mapping.edit(j);
                if (edit_is_match(edit)) {
                    gaf.matches += edit.from_length();
                }
                if (cs_cigar == true) {
                    // CS-cigar string
                    if (edit_is_match(edit)) {
                        // Merge up matches that span edits/mappings
                        running_match_length += edit.from_length();
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
                        } else if (edit_is_deletion(edit)) {
                            if (node_seq.empty()) {
                                node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                            }
                            // Deletion is - followed by deleted sequence
                            assert(offset + edit.from_length() <= node_seq.length());
                            cs_cigar_str += "-" + node_seq.substr(offset, edit.from_length());
                        } else if (edit_is_insertion(edit)) {
                            // Insertion is "+" followed by inserted sequence
                            cs_cigar_str += "+" + edit.sequence();
                        }
                    }
                }
                offset += edit.from_length();
                total_to_len += edit.to_length();
            }

            if (i < aln.path().mapping_size() - 1 && offset != node_to_length(position.node_id())) {
                if (mapping.position().node_id() != aln.path().mapping(i + 1).position().node_id() ||
                    mapping.position().is_reverse() != aln.path().mapping(i + 1).position().is_reverse()) {
                    // we are hopping off the middle of a node, need to gobble it up with a deletion
                    if (node_seq.empty()) {
                        node_seq = node_to_sequence(position.node_id(), position.is_reverse());
                    }
                    if (running_match_length > 0) {
                        // Matches are : followed by the match length
                        cs_cigar_str += ":" + std::to_string(running_match_length);
                        running_match_length = 0;
                    }
                    cs_cigar_str += "-" + node_seq.substr(offset);
                } else {
                    // we have a duplicate node mapping.  vg map actually produces these sometimes
                    // where an insert gets its own mapping even though its from_length is 0
                    // the gaf cigar format assumes nodes are fully covered, so we squish it out.
                    skip_step = true;
                }
            }
            
            //6 string Path matching /([><][^\s><]+(:\d+-\d+)?)+|([^\s><]+)/
            if (!skip_step) {
                auto& position = mapping.position();
                gafkluge::GafStep step;
                step.name = std::to_string(position.node_id());
                step.is_stable = false;
                step.is_reverse = position.is_reverse();
                step.is_interval = false;
                gaf.path_length += node_to_length(position.node_id());
                if (i == 0) {
                    gaf.path_start = position.offset();
                }
                gaf.path.push_back(std::move(step));
            }
            
            if (i == aln.path().mapping_size()-1) {
                //9 int End position on the path (0-based)
                gaf.path_end = offset;
                assert(gaf.path_end >= 0);
            }

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

        //11 int Alignment block length
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
                
    }

    return gaf;
    
}

gafkluge::GafRecord alignment_to_gaf(const HandleGraph& graph, const Alignment& aln, bool cs_cigar, bool base_quals) {
    function<size_t(nid_t)> node_to_length = [&graph](nid_t node_id) {
        return graph.get_length(graph.get_handle(node_id));
    };
    function<string(nid_t, bool)> node_to_sequence = [&graph](nid_t node_id, bool is_reversed) {
        return graph.get_sequence(graph.get_handle(node_id, is_reversed));
    };
    return alignment_to_gaf(node_to_length, node_to_sequence, aln, cs_cigar, base_quals);
}

void gaf_to_alignment(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const gafkluge::GafRecord& gaf, Alignment& aln){

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
        // Use the CS cigar string to add Edits into our Path, as well as set the sequence
        gafkluge::for_each_cs(gaf, [&] (const string& cs_cigar) {
                assert(cur_offset < cur_len || (cs_cigar[0] == '+' && cur_offset <= cur_len));

                if (cs_cigar[0] == ':') {
                    int64_t match_len = stol(cs_cigar.substr(1));
                    while (match_len > 0) {
                        int64_t current_match = std::min(match_len, (int64_t)node_to_length(cur_position.node_id()) - cur_offset);
                        Edit* edit = aln.mutable_path()->mutable_mapping(cur_mapping)->add_edit();
                        edit->set_from_length(current_match);
                        edit->set_to_length(current_match);
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
                } else if (cs_cigar[0] == '+') {
                    size_t tgt_mapping = cur_mapping;
                    // left-align insertions to try to be more consistent with vg
                    if (cur_offset == 0 && cur_mapping > 0 && (!aln.path().mapping(cur_mapping - 1).position().is_reverse()
                                                               || cur_mapping == aln.path().mapping_size())) {
                        --tgt_mapping;
                    }
                    Edit* edit = aln.mutable_path()->mutable_mapping(tgt_mapping)->add_edit();
                    edit->set_from_length(0);
                    edit->set_to_length(cs_cigar.length() - 1);
                    edit->set_sequence(cs_cigar.substr(1));
                    sequence += edit->sequence();
                } else if (cs_cigar[0] == '-') {
                    string del = cs_cigar.substr(1);
                    assert(del.length() <= node_to_length(cur_position.node_id()) - cur_offset);
                    assert(!node_to_sequence || del == node_to_sequence(cur_position.node_id(), cur_position.is_reverse()).substr(cur_offset, del.length()));
                    Edit* edit = aln.mutable_path()->mutable_mapping(cur_mapping)->add_edit();
                    edit->set_to_length(0);
                    edit->set_from_length(del.length());
                    cur_offset += del.length();
                    // unlike matches, we don't allow deletions to span multiple nodes
                    assert(cur_offset <= node_to_length(cur_position.node_id()));
                } else if (cs_cigar[0] == '*') {
                    assert(cs_cigar.length() == 3);
                    char from = cs_cigar[1];
                    char to = cs_cigar[2];
                    assert(!node_to_sequence || node_to_sequence(cur_position.node_id(), cur_position.is_reverse())[cur_offset] == from);
                    Edit* edit = aln.mutable_path()->mutable_mapping(cur_mapping)->add_edit();
                    // todo: support multibase snps
                    edit->set_from_length(1);
                    edit->set_to_length(1);
                    edit->set_sequence(string(1, to));
                    sequence += edit->sequence();
                    ++cur_offset;
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
        }
    }
}

void gaf_to_alignment(const HandleGraph& graph, const gafkluge::GafRecord& gaf, Alignment& aln) {
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
