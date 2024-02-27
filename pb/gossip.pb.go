// Code generated by protoc-gen-go. DO NOT EDIT.
// source: pb/gossip.proto

package kronospb

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Request struct {
	// node_id is the id of the node that sent the request
	NodeId string `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	// addr is the address of the node that sent the request
	AdvertisedHostPort string `protobuf:"bytes,2,opt,name=advertised_host_port,json=advertisedHostPort,proto3" json:"advertised_host_port,omitempty"`
	// cluster_id is the id of the cluster that sent the request
	ClusterId            string           `protobuf:"bytes,3,opt,name=cluster_id,json=clusterId,proto3" json:"cluster_id,omitempty"`
	GossipMap            map[string]*Info `protobuf:"bytes,4,rep,name=gossipMap,proto3" json:"gossipMap,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *Request) Reset()         { *m = Request{} }
func (m *Request) String() string { return proto.CompactTextString(m) }
func (*Request) ProtoMessage()    {}
func (*Request) Descriptor() ([]byte, []int) {
	return fileDescriptor_gossip_db7e523798fb3f79, []int{0}
}
func (m *Request) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Request.Unmarshal(m, b)
}
func (m *Request) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Request.Marshal(b, m, deterministic)
}
func (dst *Request) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Request.Merge(dst, src)
}
func (m *Request) XXX_Size() int {
	return xxx_messageInfo_Request.Size(m)
}
func (m *Request) XXX_DiscardUnknown() {
	xxx_messageInfo_Request.DiscardUnknown(m)
}

var xxx_messageInfo_Request proto.InternalMessageInfo

func (m *Request) GetNodeId() string {
	if m != nil {
		return m.NodeId
	}
	return ""
}

func (m *Request) GetAdvertisedHostPort() string {
	if m != nil {
		return m.AdvertisedHostPort
	}
	return ""
}

func (m *Request) GetClusterId() string {
	if m != nil {
		return m.ClusterId
	}
	return ""
}

func (m *Request) GetGossipMap() map[string]*Info {
	if m != nil {
		return m.GossipMap
	}
	return nil
}

type Info struct {
	// timestamp is the time at which the info was added
	Timestamp int64 `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	// data is the actual data
	Data                 []byte   `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Info) Reset()         { *m = Info{} }
func (m *Info) String() string { return proto.CompactTextString(m) }
func (*Info) ProtoMessage()    {}
func (*Info) Descriptor() ([]byte, []int) {
	return fileDescriptor_gossip_db7e523798fb3f79, []int{1}
}
func (m *Info) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Info.Unmarshal(m, b)
}
func (m *Info) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Info.Marshal(b, m, deterministic)
}
func (dst *Info) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Info.Merge(dst, src)
}
func (m *Info) XXX_Size() int {
	return xxx_messageInfo_Info.Size(m)
}
func (m *Info) XXX_DiscardUnknown() {
	xxx_messageInfo_Info.DiscardUnknown(m)
}

var xxx_messageInfo_Info proto.InternalMessageInfo

func (m *Info) GetTimestamp() int64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *Info) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type Response struct {
	// node_id is the id of the node that sent the response
	NodeId               string           `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	Data                 map[string]*Info `protobuf:"bytes,2,rep,name=data,proto3" json:"data,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	XXX_NoUnkeyedLiteral struct{}         `json:"-"`
	XXX_unrecognized     []byte           `json:"-"`
	XXX_sizecache        int32            `json:"-"`
}

func (m *Response) Reset()         { *m = Response{} }
func (m *Response) String() string { return proto.CompactTextString(m) }
func (*Response) ProtoMessage()    {}
func (*Response) Descriptor() ([]byte, []int) {
	return fileDescriptor_gossip_db7e523798fb3f79, []int{2}
}
func (m *Response) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Response.Unmarshal(m, b)
}
func (m *Response) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Response.Marshal(b, m, deterministic)
}
func (dst *Response) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Response.Merge(dst, src)
}
func (m *Response) XXX_Size() int {
	return xxx_messageInfo_Response.Size(m)
}
func (m *Response) XXX_DiscardUnknown() {
	xxx_messageInfo_Response.DiscardUnknown(m)
}

var xxx_messageInfo_Response proto.InternalMessageInfo

func (m *Response) GetNodeId() string {
	if m != nil {
		return m.NodeId
	}
	return ""
}

func (m *Response) GetData() map[string]*Info {
	if m != nil {
		return m.Data
	}
	return nil
}

type NodeDescriptor struct {
	// node_id is the id of the node
	NodeId string `protobuf:"bytes,1,opt,name=node_id,json=nodeId,proto3" json:"node_id,omitempty"`
	// grpc_addr is the grpc address of the node
	GrpcAddr string `protobuf:"bytes,2,opt,name=grpc_addr,json=grpcAddr,proto3" json:"grpc_addr,omitempty"`
	// raft_addr is the raft address of the node
	RaftAddr             string   `protobuf:"bytes,3,opt,name=raft_addr,json=raftAddr,proto3" json:"raft_addr,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *NodeDescriptor) Reset()         { *m = NodeDescriptor{} }
func (m *NodeDescriptor) String() string { return proto.CompactTextString(m) }
func (*NodeDescriptor) ProtoMessage()    {}
func (*NodeDescriptor) Descriptor() ([]byte, []int) {
	return fileDescriptor_gossip_db7e523798fb3f79, []int{3}
}
func (m *NodeDescriptor) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_NodeDescriptor.Unmarshal(m, b)
}
func (m *NodeDescriptor) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_NodeDescriptor.Marshal(b, m, deterministic)
}
func (dst *NodeDescriptor) XXX_Merge(src proto.Message) {
	xxx_messageInfo_NodeDescriptor.Merge(dst, src)
}
func (m *NodeDescriptor) XXX_Size() int {
	return xxx_messageInfo_NodeDescriptor.Size(m)
}
func (m *NodeDescriptor) XXX_DiscardUnknown() {
	xxx_messageInfo_NodeDescriptor.DiscardUnknown(m)
}

var xxx_messageInfo_NodeDescriptor proto.InternalMessageInfo

func (m *NodeDescriptor) GetNodeId() string {
	if m != nil {
		return m.NodeId
	}
	return ""
}

func (m *NodeDescriptor) GetGrpcAddr() string {
	if m != nil {
		return m.GrpcAddr
	}
	return ""
}

func (m *NodeDescriptor) GetRaftAddr() string {
	if m != nil {
		return m.RaftAddr
	}
	return ""
}

func init() {
	proto.RegisterType((*Request)(nil), "kronospb.Request")
	proto.RegisterMapType((map[string]*Info)(nil), "kronospb.Request.GossipMapEntry")
	proto.RegisterType((*Info)(nil), "kronospb.Info")
	proto.RegisterType((*Response)(nil), "kronospb.Response")
	proto.RegisterMapType((map[string]*Info)(nil), "kronospb.Response.DataEntry")
	proto.RegisterType((*NodeDescriptor)(nil), "kronospb.NodeDescriptor")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// GossipClient is the client API for Gossip service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type GossipClient interface {
	Gossip(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error)
}

type gossipClient struct {
	cc *grpc.ClientConn
}

func NewGossipClient(cc *grpc.ClientConn) GossipClient {
	return &gossipClient{cc}
}

func (c *gossipClient) Gossip(ctx context.Context, in *Request, opts ...grpc.CallOption) (*Response, error) {
	out := new(Response)
	err := c.cc.Invoke(ctx, "/kronospb.Gossip/Gossip", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GossipServer is the server API for Gossip service.
type GossipServer interface {
	Gossip(context.Context, *Request) (*Response, error)
}

func RegisterGossipServer(s *grpc.Server, srv GossipServer) {
	s.RegisterService(&_Gossip_serviceDesc, srv)
}

func _Gossip_Gossip_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Request)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GossipServer).Gossip(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/kronospb.Gossip/Gossip",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GossipServer).Gossip(ctx, req.(*Request))
	}
	return interceptor(ctx, in, info, handler)
}

var _Gossip_serviceDesc = grpc.ServiceDesc{
	ServiceName: "kronospb.Gossip",
	HandlerType: (*GossipServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Gossip",
			Handler:    _Gossip_Gossip_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "pb/gossip.proto",
}

func init() { proto.RegisterFile("pb/gossip.proto", fileDescriptor_gossip_db7e523798fb3f79) }

var fileDescriptor_gossip_db7e523798fb3f79 = []byte{
	// 373 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xa4, 0x52, 0x41, 0x6b, 0xe2, 0x40,
	0x18, 0xdd, 0x18, 0x57, 0x93, 0xcf, 0xc5, 0xdd, 0x1d, 0x16, 0x56, 0x5c, 0x17, 0x24, 0xf4, 0xe0,
	0x29, 0x6d, 0xed, 0x45, 0x5a, 0x28, 0xb4, 0x58, 0xac, 0xd0, 0x96, 0x92, 0x63, 0x2f, 0x61, 0xcc,
	0x8c, 0x36, 0xa8, 0x99, 0xe9, 0xcc, 0xa7, 0xe0, 0xb1, 0x7f, 0xa4, 0xbf, 0xb5, 0xcc, 0x24, 0x9a,
	0x16, 0x69, 0x2f, 0xbd, 0x7d, 0x79, 0xef, 0xfb, 0x1e, 0xef, 0xe5, 0x0d, 0xfc, 0x94, 0x93, 0xc3,
	0x99, 0xd0, 0x3a, 0x95, 0xa1, 0x54, 0x02, 0x05, 0xf1, 0xe6, 0x4a, 0x64, 0x42, 0xcb, 0x49, 0xf0,
	0x5c, 0x81, 0x7a, 0xc4, 0x9f, 0x56, 0x5c, 0x23, 0xf9, 0x0b, 0xf5, 0x4c, 0x30, 0x1e, 0xa7, 0xac,
	0xe5, 0x74, 0x9d, 0x9e, 0x1f, 0xd5, 0xcc, 0xe7, 0x98, 0x91, 0x23, 0xf8, 0x43, 0xd9, 0x9a, 0x2b,
	0x4c, 0x35, 0x67, 0xf1, 0xa3, 0xd0, 0x18, 0x4b, 0xa1, 0xb0, 0x55, 0xb1, 0x5b, 0xa4, 0xe4, 0xae,
	0x85, 0xc6, 0x7b, 0xa1, 0x90, 0xfc, 0x07, 0x48, 0x16, 0x2b, 0x8d, 0x5c, 0x19, 0x35, 0xd7, 0xee,
	0xf9, 0x05, 0x32, 0x66, 0xe4, 0x1c, 0xfc, 0xdc, 0xcf, 0x2d, 0x95, 0xad, 0x6a, 0xd7, 0xed, 0x35,
	0xfa, 0xdd, 0x70, 0xeb, 0x29, 0x2c, 0xfc, 0x84, 0xa3, 0xed, 0xca, 0x55, 0x86, 0x6a, 0x13, 0x95,
	0x27, 0xed, 0x1b, 0x68, 0xbe, 0x27, 0xc9, 0x2f, 0x70, 0xe7, 0x7c, 0x53, 0xf8, 0x36, 0x23, 0x39,
	0x80, 0xef, 0x6b, 0xba, 0x58, 0x71, 0xeb, 0xb2, 0xd1, 0x6f, 0x96, 0xfa, 0xe3, 0x6c, 0x2a, 0xa2,
	0x9c, 0x3c, 0xad, 0x0c, 0x9c, 0x60, 0x00, 0x55, 0x03, 0x91, 0x0e, 0xf8, 0x98, 0x2e, 0xb9, 0x46,
	0xba, 0x94, 0x56, 0xc9, 0x8d, 0x4a, 0x80, 0x10, 0xa8, 0x32, 0x8a, 0xd4, 0xca, 0xfd, 0x88, 0xec,
	0x1c, 0xbc, 0x38, 0xe0, 0x45, 0x5c, 0x4b, 0x91, 0x69, 0xfe, 0xd9, 0xef, 0xdb, 0x5e, 0x9a, 0xa0,
	0x9d, 0xb7, 0x41, 0xf3, 0xd3, 0x70, 0x48, 0x91, 0xe6, 0x21, 0xed, 0x66, 0x7b, 0x04, 0xfe, 0x0e,
	0xfa, 0x52, 0xb4, 0x04, 0x9a, 0x77, 0x82, 0xf1, 0x21, 0xd7, 0x89, 0x4a, 0x25, 0x0a, 0xf5, 0xb1,
	0xcb, 0x7f, 0xe0, 0xcf, 0x94, 0x4c, 0x62, 0xca, 0x98, 0x2a, 0x9a, 0xf5, 0x0c, 0x70, 0xc1, 0x98,
	0x32, 0xa4, 0xa2, 0x53, 0xcc, 0xc9, 0xbc, 0x4e, 0xcf, 0x00, 0x86, 0xec, 0x9f, 0x41, 0x2d, 0x6f,
	0x83, 0x1c, 0xef, 0xa6, 0xdf, 0x7b, 0x75, 0xb6, 0xc9, 0x7e, 0xf0, 0xe0, 0xdb, 0x25, 0x3c, 0xec,
	0x1e, 0xe3, 0xa4, 0x66, 0x5f, 0xe7, 0xc9, 0x6b, 0x00, 0x00, 0x00, 0xff, 0xff, 0x97, 0xa4, 0xc9,
	0x2c, 0xb0, 0x02, 0x00, 0x00,
}
