#ifndef VG_IO_ALIGNMENT_IO_HPP_INCLUDED
#define VG_IO_ALIGNMENT_IO_HPP_INCLUDED

#include <iostream>
#include <functional>
#include <zlib.h>
#include "vg/vg.pb.h"
#include <htslib/hfile.h>
#include <htslib/hts.h>
#include <htslib/sam.h>
#include <htslib/vcf.h>
#include <handlegraph/handle_graph.hpp>
#include <handlegraph/named_node_back_translation.hpp>
#include "gafkluge.hpp"

namespace vg {

namespace io {

using handle_t = handlegraph::handle_t;
using nid_t = handlegraph::nid_t;
using path_handle_t = handlegraph::path_handle_t;
using step_handle_t = handlegraph::step_handle_t;
using edge_t = handlegraph::edge_t;
using HandleGraph = handlegraph::HandleGraph;

using namespace std;

const uint64_t DEFAULT_PARALLEL_BATCHSIZE = 512;

// general (implemented below)
template<typename T>
size_t unpaired_for_each_parallel(function<bool(T&)> get_read_if_available,
                                  function<void(T&)> lambda,
                                  uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);

template<typename T>
size_t paired_for_each_parallel_after_wait(function<bool(T&, T&)> get_pair_if_available,
                                           function<void(T&, T&)> lambda,
                                           function<bool(void)> single_threaded_until_true,
                                           uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
// single gaf
bool get_next_record_from_gaf(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, htsFile* fp, kstring_t& s_buffer, gafkluge::GafRecord& record);
bool get_next_record_pair_from_gaf(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, htsFile* fp, kstring_t& s_buffer,
                                   gafkluge::GafRecord& mate1, gafkluge::GafRecord& mate2);
size_t gaf_unpaired_for_each(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename, function<void(Alignment&)> lambda);
size_t gaf_unpaired_for_each(const HandleGraph& graph, const string& filename, function<void(Alignment&)> lambda);
size_t gaf_paired_interleaved_for_each(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                       function<void(Alignment&, Alignment&)> lambda);
size_t gaf_paired_interleaved_for_each(const HandleGraph& graph, const string& filename,
                                       function<void(Alignment&, Alignment&)> lambda);

// parallel gaf
size_t gaf_unpaired_for_each_parallel(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                      function<void(Alignment&)> lambda,
                                      uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
size_t gaf_unpaired_for_each_parallel(const HandleGraph& graph, const string& filename,
                                      function<void(Alignment&)> lambda,
                                      uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
size_t gaf_paired_interleaved_for_each_parallel(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                                function<void(Alignment&, Alignment&)> lambda,
                                                uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
size_t gaf_paired_interleaved_for_each_parallel(const HandleGraph& graph, const string& filename,
                                                function<void(Alignment&, Alignment&)> lambda,
                                                uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
size_t gaf_paired_interleaved_for_each_parallel_after_wait(function<size_t(nid_t)> node_to_length, function<string(nid_t, bool)> node_to_sequence, const string& filename,
                                                           function<void(Alignment&, Alignment&)> lambda,
                                                           function<bool(void)> single_threaded_until_true,
                                                           uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
size_t gaf_paired_interleaved_for_each_parallel_after_wait(const HandleGraph& graph, const string& filename,
                                                           function<void(Alignment&, Alignment&)> lambda,
                                                           function<bool(void)> single_threaded_until_true,
                                                           uint64_t batch_size = DEFAULT_PARALLEL_BATCHSIZE);
// gaf conversion

/// Convert an alignment to GAF. The alignment must be in node ID space.
/// If translate_through is set, output will be in segment name space.
// If cs_cigar is true, will store a CIGAR string in the cs tag in the GAF.
gafkluge::GafRecord alignment_to_gaf(function<size_t(nid_t)> node_to_length,
                                     function<string(nid_t, bool)> node_to_sequence,
                                     const Alignment& aln,
                                     const handlegraph::NamedNodeBackTranslation* translate_through = nullptr,
                                     bool cs_cigar = true,
                                     bool base_quals = true,
                                     bool frag_links = true);
/// Convert an alignment to GAF. The alignment must be in node ID space.
/// If translate_through is set, output will be in segment name space.
// If cs_cigar is true, will store a CIGAR string in the cs tag in the GAF.
gafkluge::GafRecord alignment_to_gaf(const HandleGraph& graph,
                                     const Alignment& aln,
                                     const handlegraph::NamedNodeBackTranslation* translate_through = nullptr,
                                     bool cs_cigar = true,
                                     bool base_quals = true,
                                     bool frag_links = true);
// TODO: These will need to be able to take a forward translation to read named-segment GAF.
/// Convert a GAF alignment into a vg Alignment. The alignment must be in node ID space.
void gaf_to_alignment(function<size_t(nid_t)> node_to_length,
                      function<string(nid_t, bool)> node_to_sequence,
                      const gafkluge::GafRecord& gaf,
                      Alignment& aln);
/// Convert a GAF alignment into a vg Alignment. The alignment must be in node ID space.
void gaf_to_alignment(const HandleGraph& graph,
                      const gafkluge::GafRecord& gaf,
                      Alignment& aln);

// utility
short quality_char_to_short(char c);
char quality_short_to_char(short i);
string string_quality_char_to_short(const string& quality);
string string_quality_short_to_char(const string& quality);
void alignment_quality_char_to_short(Alignment& alignment);
void alignment_quality_short_to_char(Alignment& alignment);

// implementation
template<typename T>
inline size_t unpaired_for_each_parallel(function<bool(T&)> get_read_if_available,
                                         function<void(T&)> lambda,
                                         uint64_t batch_size) {
    assert(batch_size % 2 == 0);    
    size_t nLines = 0;
    vector<T> *batch = nullptr;
    // number of batches currently being processed
    uint64_t batches_outstanding = 0;
#pragma omp parallel default(none) shared(batches_outstanding, batch, nLines, get_read_if_available, lambda, batch_size)
#pragma omp single
    {
        
        // max # of such batches to be holding in memory
        uint64_t max_batches_outstanding = batch_size;
        // max # we will ever increase the batch buffer to
        const uint64_t max_max_batches_outstanding = 1 << 13; // 8192
        
        // alignments to hold the incoming data
        T aln;
        // did we find the end of the file yet?
        bool more_data = true;
        
        while (more_data) {
            // init a new batch
            batch = new std::vector<T>();
            batch->reserve(batch_size);
            
            // load up to the batch-size number of reads
            for (int i = 0; i < batch_size; i++) {
                
                more_data = get_read_if_available(aln);
                
                if (more_data) {
                    batch->emplace_back(std::move(aln));
                    nLines++;
                }
                else {
                    break;
                }
            }
            
            // did we get a batch?
            if (batch->size()) {
                
                // how many batch tasks are outstanding currently, including this one?
                uint64_t current_batches_outstanding;
#pragma omp atomic capture
                current_batches_outstanding = ++batches_outstanding;
                
                if (current_batches_outstanding >= max_batches_outstanding) {
                    // do this batch in the current thread because we've spawned the maximum number of
                    // concurrent batch tasks
                    for (auto& aln : *batch) {
                        lambda(aln);
                    }
                    delete batch;
#pragma omp atomic capture
                    current_batches_outstanding = --batches_outstanding;
                    
                    if (4 * current_batches_outstanding / 3 < max_batches_outstanding
                        && max_batches_outstanding < max_max_batches_outstanding) {
                        // we went through at least 1/4 of the batch buffer while we were doing this thread's batch
                        // this looks risky, since we want the batch buffer to stay populated the entire time we're
                        // occupying this thread on compute, so let's increase the batch buffer size
                        
                        max_batches_outstanding *= 2;
                    }
                }
                else {
                    // spawn a new task to take care of this batch
#pragma omp task default(none) firstprivate(batch) shared(batches_outstanding, lambda)
                    {
                        for (auto& aln : *batch) {
                            lambda(aln);
                        }
                        delete batch;
#pragma omp atomic update
                        batches_outstanding--;
                    }
                }
            }
        }
    }
    return nLines;
}

template<typename T>
inline size_t paired_for_each_parallel_after_wait(function<bool(T&, T&)> get_pair_if_available,
                                                  function<void(T&, T&)> lambda,
                                                  function<bool(void)> single_threaded_until_true,
                                                  uint64_t batch_size) {

    assert(batch_size % 2 == 0);
    size_t nLines = 0;
    vector<pair<T, T> > *batch = nullptr;
    // number of batches currently being processed
    uint64_t batches_outstanding = 0;
    
#pragma omp parallel default(none) shared(batches_outstanding, batch, nLines, get_pair_if_available, single_threaded_until_true, lambda, batch_size)
#pragma omp single
    {

        // max # of such batches to be holding in memory
        uint64_t max_batches_outstanding = batch_size;
        // max # we will ever increase the batch buffer to
        const uint64_t max_max_batches_outstanding = 1 << 13; // 8192
        
        // alignments to hold the incoming data
        T mate1, mate2;
        // did we find the end of the file yet?
        bool more_data = true;
        
        while (more_data) {
            // init a new batch
            batch = new std::vector<pair<T, T>>();
            batch->reserve(batch_size);
            
            // load up to the batch-size number of pairs
            for (int i = 0; i < batch_size; i++) {
                
                more_data = get_pair_if_available(mate1, mate2);
                
                if (more_data) {
                    batch->emplace_back(std::move(mate1), std::move(mate2));
                    nLines++;
                }
                else {
                    break;
                }
            }
            
            // did we get a batch?
            if (batch->size()) {
                // how many batch tasks are outstanding currently, including this one?
                uint64_t current_batches_outstanding;
#pragma omp atomic capture
                current_batches_outstanding = ++batches_outstanding;
                
                bool do_single_threaded = !single_threaded_until_true();
                if (current_batches_outstanding >= max_batches_outstanding || do_single_threaded) {
                    // do this batch in the current thread because we've spawned the maximum number of
                    // concurrent batch tasks or because we are directed to work in a single thread
                    for (auto& p : *batch) {
                        lambda(p.first, p.second);
                    }
                    delete batch;
#pragma omp atomic capture
                    current_batches_outstanding = --batches_outstanding;
                    
                    if (4 * current_batches_outstanding / 3 < max_batches_outstanding
                        && max_batches_outstanding < max_max_batches_outstanding
                        && !do_single_threaded) {
                        // we went through at least 1/4 of the batch buffer while we were doing this thread's batch
                        // this looks risky, since we want the batch buffer to stay populated the entire time we're
                        // occupying this thread on compute, so let's increase the batch buffer size
                        // (skip this adjustment if you're in single-threaded mode and thus expect the buffer to be
                        // empty)
                        
                        max_batches_outstanding *= 2;
                    }
                }
                else {
                    // spawn a new task to take care of this batch
#pragma omp task default(none) firstprivate(batch) shared(batches_outstanding, lambda)
                    {
                        for (auto& p : *batch) {
                            lambda(p.first, p.second);
                        }
                        delete batch;
#pragma omp atomic update
                        batches_outstanding--;
                    }
                }
            }
        }
    }
    
    return nLines;
}

}
}
#endif
