// std
#include <fstream>  // ifstream
#include <functional>  // function
#include <iostream>  // cout
#include <string>
// local
#include "vg/io/basic_stream.hpp"
#include "vg/io/stream.hpp"



using std::cout;
using std::function;
using std::ifstream;
using std::string;
using vg::Edge;
using vg::Graph;
using vg::Node;

namespace vg {
namespace io {

void mergeGraphs(Graph& graph, const Graph& part)
{
  for (const Node& node : part.node()) {
    Node* n = graph.add_node();
    n->set_id(node.id());
    n->set_sequence(node.sequence());
    n->set_name(node.name());
  }
  for (const Edge& edge : part.edge()) {
    Edge* e = graph.add_edge();
    e->set_from(edge.from());
    e->set_to(edge.to());
    e->set_from_start(edge.from_start());
    e->set_to_end(edge.to_end());
    e->set_overlap(edge.overlap());
  }
  for (const Path& path : part.path()) {
    Path* p = graph.add_path();
    p->set_name(path.name());
    p->set_is_circular(path.is_circular());
    p->set_length(path.length());
    for (const Mapping& mapping : path.mapping()) {
      Mapping* m = p->add_mapping();
      m->set_rank(mapping.rank());
      const Position& position = mapping.position();
      Position* mp = m->mutable_position();
      mp->set_node_id(position.node_id());
      mp->set_offset(position.offset());
      mp->set_is_reverse(position.is_reverse());
      for (const Edit& edit : mapping.edit()) {
        Edit* e = m->add_edit();
        e->set_from_length(edit.from_length());
        e->set_to_length(edit.to_length());
        e->set_sequence(edit.sequence());
      }
    }
  }
}

Graph inputStream(const string& filename)
{   
    Graph result;
    ifstream graphfile { filename, std::ios::in | std::ios::binary };
    function<void(Graph&)> lambda = [&result](Graph& g) {
      mergeGraphs(result, g); 
    };
    for_each(graphfile, lambda);
    return result;
}

void outputStream(const Graph& g)
{
  function<Graph(uint64_t)> lambda = [&g](uint64_t i) -> Graph {
    return g;
  };
  write<Graph>(cout, 1, lambda);
}

} // io
} // vg
