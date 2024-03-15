#include <iostream>
#include <string>

#include "vg/vg.pb.h"
#include <google/protobuf/descriptor.h>

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
    
    std::cerr << "Tests complete!" << std::endl;
    return 0;
}
