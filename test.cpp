#include <iostream>
#include <string>

#include "vg/vg.pb.h"
#include <google/protobuf/descriptor.h>

#include "vg/io/alignment_io.hpp"
#include "vg/io/protobuf_emitter.hpp"

#include <algorithm>
#include <fstream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <cstdio>
#include <unistd.h>
#include <omp.h>

namespace {
using namespace vg;
using namespace vg::io;
using Groups = std::vector<std::vector<std::string>>;

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

void check_groups(Groups observed, Groups expected, size_t count) {
    // Callback completion order is unspecified, but order within a run is not.
    std::sort(observed.begin(), observed.end());
    std::sort(expected.begin(), expected.end());
    require(count == expected.size(), "wrong group count");
    require(observed == expected, "groups lost, split, combined, or reordered records");
}

struct TemporaryGaf {
    std::string path;
    TemporaryGaf() {
        char pattern[] = "/tmp/libvgio-grouped-XXXXXX";
        int fd = mkstemp(pattern);
        if (fd < 0) {
            throw std::runtime_error("could not create GAF fixture");
        }
        close(fd);
        path = pattern;
    }
    ~TemporaryGaf() { std::remove(path.c_str()); }
};
}

void test_grouped_input() {
    // A repeated key after another key starts a new group; the final A is flushed at EOF.
    const std::vector<std::string> input{"a:0", "a:1", "b:2", "a:3"};
    size_t cursor = 0;
    Groups observed;
    auto count = grouped_for_each_parallel<std::string>(
        [&](std::string& record) {
            if (cursor == input.size()) return false;
            record = input[cursor++];
            return true;
        },
        [](const std::string& record) { return record.substr(0, record.find(':')); },
        [&](std::vector<std::string>& group) { observed.push_back(group); }, 2);
    check_groups(observed, {{"a:0", "a:1"}, {"b:2"}, {"a:3"}}, count);

    observed.clear();
    count = grouped_for_each_parallel<std::string>(
        [](std::string&) { return false; },
        [](const std::string& record) { return record; },
        [&](std::vector<std::string>& group) { observed.push_back(group); }, 2);
    check_groups(observed, {}, count);

    std::cerr << "Consecutive grouping and EOF tests passed." << std::endl;
}

void test_parallel_grouped_input() {
    omp_set_num_threads(4);
    // Nine groups fill four two-group batches and a partial final batch.
    // Each group has three records: the batch limit counts groups, not records.
    Groups expected;
    std::vector<std::string> input;
    for (size_t i = 0; i < 9; ++i) {
        const auto name = std::to_string(i);
        expected.push_back({name + ":0", name + ":1", name + ":2"});
        input.insert(input.end(), expected.back().begin(), expected.back().end());
    }
    size_t cursor = 0;
    Groups observed;
    std::mutex mutex;
    auto count = grouped_for_each_parallel<std::string>(
        [&](std::string& record) {
            if (cursor == input.size()) return false;
            record = input[cursor++];
            return true;
        },
        [](const std::string& record) { return record.substr(0, record.find(':')); },
        [&](std::vector<std::string>& group) {
            std::lock_guard<std::mutex> lock(mutex);
            observed.push_back(group);
        }, 2);
    check_groups(observed, expected, count);
    omp_set_num_threads(1);
    std::cerr << "Parallel group integrity test passed." << std::endl;
}

void test_grouped_gam_input() {
    std::stringstream gam;
    {
        ProtobufEmitter<Alignment> emitter(gam);
        for (int score : {10, 20, 30}) {
            Alignment aln;
            aln.set_name(score == 30 ? "b" : "a");
            aln.set_sequence("ACGT");
            aln.set_score(score);
            emitter.write(std::move(aln));
        }
    }
    std::vector<std::vector<Alignment>> observed;
    auto count = gam_grouped_for_each_parallel(gam,
        [&](std::vector<Alignment>& group) { observed.push_back(std::move(group)); }, 2);
    Groups names_and_scores;
    for (const auto& group : observed) {
        names_and_scores.emplace_back();
        for (const auto& aln : group) {
            require(aln.sequence() == "ACGT", "GAM sequence changed");
            names_and_scores.back().push_back(aln.name() + ":" + std::to_string(aln.score()));
        }
    }
    check_groups(names_and_scores, {{"a:10", "a:20"}, {"b:30"}}, count);
    std::cerr << "Grouped GAM adapter test passed." << std::endl;
}

void test_grouped_gaf_input() {
    TemporaryGaf fixture;
    {
        std::ofstream gaf(fixture.path);
        gaf << "a\t4\t0\t4\t+\t>1\t4\t0\t4\t4\t4\t60\tcs:Z::4\tAS:i:10\ttp:A:P\n"
            << "a\t4\t0\t4\t+\t>1\t4\t0\t4\t4\t4\t60\tcs:Z::4\tAS:i:20\ttp:A:S\n"
            << "b\t4\t0\t4\t+\t>1\t4\t0\t4\t4\t4\t60\tcs:Z::4\tAS:i:30\ttp:A:P\n";
    }
    std::vector<std::vector<Alignment>> observed;
    auto count = gaf_grouped_for_each_parallel(
        [](nid_t) { return size_t(4); },
        [](nid_t, bool) { return std::string("ACGT"); }, fixture.path,
        [&](std::vector<Alignment>& group) { observed.push_back(std::move(group)); }, 2);
    Groups names_and_scores;
    for (const auto& group : observed) {
        names_and_scores.emplace_back();
        for (const auto& aln : group) {
            require(aln.sequence() == "ACGT", "GAF sequence reconstruction failed");
            require(aln.path().mapping_size() == 1 && aln.path().mapping(0).position().node_id() == 1,
                    "GAF graph placement was lost");
            require(aln.is_secondary() == (aln.score() == 20), "GAF primary/secondary status was lost");
            names_and_scores.back().push_back(aln.name() + ":" + std::to_string(aln.score()));
        }
    }
    check_groups(names_and_scores, {{"a:10", "a:20"}, {"b:30"}}, count);
    std::cerr << "Grouped GAF adapter test passed." << std::endl;
}

void test_diploid_tags() {
    auto length = [](nid_t) -> size_t { return 4; };
    auto sequence = [](nid_t, bool) { return string("ACGT"); };
    for (bool preferred : {false, true}) {
        for (int quality : {0, 60, 90, 255}) {
            Alignment original;
            original.set_name("diploid");
            original.set_sequence("ACGT");
            original.set_is_secondary(!preferred);
            auto* mapping = original.mutable_path()->add_mapping();
            mapping->mutable_position()->set_node_id(1);
            mapping->add_edit()->set_from_length(4);
            mapping->mutable_edit(0)->set_to_length(4);
            require(decode_diploid_tag(original, "hp", 'Z', preferred ? "pri_hap" : "sec_hap"), "hp decoding failed");
            require(decode_diploid_tag(original, "hq", 'i', "0"), "hq decoding failed");
            require(decode_diploid_tag(original, "aq", 'i', to_string(quality)), "aq decoding failed");
            (*original.mutable_annotation()->mutable_fields())["tags"].set_string_value("ZZ:Z:keep\thp:Z:obsolete\taq:i:1");
            auto gaf = alignment_to_gaf(length, sequence, original);
            require(gaf.opt_fields.at("aq").second == to_string(quality), "typed aq did not override raw tag");
            Alignment restored;
            gaf_to_alignment(length, sequence, gaf, restored);
            require(encode_diploid_tags(restored) == encode_diploid_tags(original), "diploid GAF round trip failed");
            require(restored.is_secondary() == original.is_secondary(), "GAF primary status was lost");
            require(restored.annotation().fields().at("tags").string_value() == "ZZ:Z:keep", "raw tag preservation failed");
        }
    }
    Alignment alignment;
    for (const string& value : {"", "-1", "256", "1.5", "60junk", "999999999999999999999"}) {
        require(!decode_diploid_tag(alignment, "aq", 'i', value), "invalid quality accepted");
    }
    require(!decode_diploid_tag(alignment, "hp", 'i', "1"), "unrelated hp type consumed");
    require(!decode_diploid_tag(alignment, "hp", 'Z', "other"), "unrelated hp value consumed");
    (*alignment.mutable_annotation()->mutable_fields())["diploid_source_mapping_quality"].set_number_value(1.5);
    bool rejected = false;
    try { encode_diploid_tags(alignment); } catch (const invalid_argument&) { rejected = true; }
    require(rejected, "invalid typed quality accepted");
}

int main (int arcg, char** argv) {
    std::cerr << "Testing libvgio..." << std::endl;
    
    std::cerr << "Creating Graph..." << std::endl;
    vg::Graph g;
    std::cerr << "Graph exists at " << &g << std::endl;
    
    // Check up on Protobuf
    std::cerr << "Checking for Protobuf descriptor pool..." << std::endl;
    const google::protobuf::DescriptorPool* pool =  google::protobuf::DescriptorPool::generated_pool();
    if (pool == nullptr) {
        throw std::runtime_error("Cound not find Protobuf descriptor pool: is libvgio working?");
    }
    std::cerr << "Found descriptor pool at " << pool << std::endl;
    
    for (auto message_name : {"vg.Graph", "vg.Alignment", "vg.Position"}) {
        std::cerr << "Checking for Protobuf message type " << message_name << "..." << std::endl;
        const google::protobuf::Descriptor* descriptor = pool->FindMessageTypeByName(message_name);
        if (descriptor == nullptr) {
            throw std::runtime_error(std::string("Cound not find Protobuf descriptor for message type ") + message_name + ": is libvgio working?");
        }
        std::cerr << "Found " << message_name << " as " << descriptor->full_name() << " at " << descriptor << std::endl;
    }
    
    const int previous_threads = omp_get_max_threads();
    omp_set_num_threads(1);
    test_grouped_input();
    test_parallel_grouped_input();
    test_grouped_gam_input();
    test_grouped_gaf_input();
    omp_set_num_threads(previous_threads);
    test_diploid_tags();

    std::cerr << "Tests complete!" << std::endl;
    return 0;
}
