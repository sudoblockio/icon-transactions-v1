// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.6.1
// source: transaction_raw.proto

package models

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TransactionRaw struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Type                      string `protobuf:"bytes,1,opt,name=type,proto3" json:"type"`
	Version                   string `protobuf:"bytes,2,opt,name=version,proto3" json:"version"`
	FromAddress               string `protobuf:"bytes,3,opt,name=from_address,json=fromAddress,proto3" json:"from_address"`
	ToAddress                 string `protobuf:"bytes,4,opt,name=to_address,json=toAddress,proto3" json:"to_address"`
	Value                     string `protobuf:"bytes,5,opt,name=value,proto3" json:"value"`
	StepLimit                 uint64 `protobuf:"varint,6,opt,name=step_limit,json=stepLimit,proto3" json:"step_limit"`
	Timestamp                 string `protobuf:"bytes,7,opt,name=timestamp,proto3" json:"timestamp"`
	BlockTimestamp            uint64 `protobuf:"varint,8,opt,name=block_timestamp,json=blockTimestamp,proto3" json:"block_timestamp"`
	Nid                       uint32 `protobuf:"varint,9,opt,name=nid,proto3" json:"nid"`
	Nonce                     string `protobuf:"bytes,10,opt,name=nonce,proto3" json:"nonce"`
	Hash                      string `protobuf:"bytes,11,opt,name=hash,proto3" json:"hash"`
	TransactionIndex          uint32 `protobuf:"varint,12,opt,name=transaction_index,json=transactionIndex,proto3" json:"transaction_index"`
	BlockHash                 string `protobuf:"bytes,13,opt,name=block_hash,json=blockHash,proto3" json:"block_hash"`
	BlockNumber               uint64 `protobuf:"varint,14,opt,name=block_number,json=blockNumber,proto3" json:"block_number"`
	Fee                       uint64 `protobuf:"varint,15,opt,name=fee,proto3" json:"fee"`
	Signature                 string `protobuf:"bytes,16,opt,name=signature,proto3" json:"signature"`
	DataType                  string `protobuf:"bytes,17,opt,name=data_type,json=dataType,proto3" json:"data_type"`
	Data                      string `protobuf:"bytes,18,opt,name=data,proto3" json:"data"`
	ReceiptCumulativeStepUsed uint64 `protobuf:"varint,19,opt,name=receipt_cumulative_step_used,json=receiptCumulativeStepUsed,proto3" json:"receipt_cumulative_step_used"`
	ReceiptStepUsed           uint64 `protobuf:"varint,20,opt,name=receipt_step_used,json=receiptStepUsed,proto3" json:"receipt_step_used"`
	ReceiptStepPrice          uint64 `protobuf:"varint,21,opt,name=receipt_step_price,json=receiptStepPrice,proto3" json:"receipt_step_price"`
	ReceiptScoreAddress       string `protobuf:"bytes,22,opt,name=receipt_score_address,json=receiptScoreAddress,proto3" json:"receipt_score_address"`
	ReceiptLogs               string `protobuf:"bytes,23,opt,name=receipt_logs,json=receiptLogs,proto3" json:"receipt_logs"`
	ReceiptStatus             uint32 `protobuf:"varint,24,opt,name=receipt_status,json=receiptStatus,proto3" json:"receipt_status"`
	ItemId                    string `protobuf:"bytes,25,opt,name=item_id,json=itemId,proto3" json:"item_id"`
	ItemTimestamp             string `protobuf:"bytes,26,opt,name=item_timestamp,json=itemTimestamp,proto3" json:"item_timestamp"`
}

func (x *TransactionRaw) Reset() {
	*x = TransactionRaw{}
	if protoimpl.UnsafeEnabled {
		mi := &file_transaction_raw_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TransactionRaw) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TransactionRaw) ProtoMessage() {}

func (x *TransactionRaw) ProtoReflect() protoreflect.Message {
	mi := &file_transaction_raw_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TransactionRaw.ProtoReflect.Descriptor instead.
func (*TransactionRaw) Descriptor() ([]byte, []int) {
	return file_transaction_raw_proto_rawDescGZIP(), []int{0}
}

func (x *TransactionRaw) GetType() string {
	if x != nil {
		return x.Type
	}
	return ""
}

func (x *TransactionRaw) GetVersion() string {
	if x != nil {
		return x.Version
	}
	return ""
}

func (x *TransactionRaw) GetFromAddress() string {
	if x != nil {
		return x.FromAddress
	}
	return ""
}

func (x *TransactionRaw) GetToAddress() string {
	if x != nil {
		return x.ToAddress
	}
	return ""
}

func (x *TransactionRaw) GetValue() string {
	if x != nil {
		return x.Value
	}
	return ""
}

func (x *TransactionRaw) GetStepLimit() uint64 {
	if x != nil {
		return x.StepLimit
	}
	return 0
}

func (x *TransactionRaw) GetTimestamp() string {
	if x != nil {
		return x.Timestamp
	}
	return ""
}

func (x *TransactionRaw) GetBlockTimestamp() uint64 {
	if x != nil {
		return x.BlockTimestamp
	}
	return 0
}

func (x *TransactionRaw) GetNid() uint32 {
	if x != nil {
		return x.Nid
	}
	return 0
}

func (x *TransactionRaw) GetNonce() string {
	if x != nil {
		return x.Nonce
	}
	return ""
}

func (x *TransactionRaw) GetHash() string {
	if x != nil {
		return x.Hash
	}
	return ""
}

func (x *TransactionRaw) GetTransactionIndex() uint32 {
	if x != nil {
		return x.TransactionIndex
	}
	return 0
}

func (x *TransactionRaw) GetBlockHash() string {
	if x != nil {
		return x.BlockHash
	}
	return ""
}

func (x *TransactionRaw) GetBlockNumber() uint64 {
	if x != nil {
		return x.BlockNumber
	}
	return 0
}

func (x *TransactionRaw) GetFee() uint64 {
	if x != nil {
		return x.Fee
	}
	return 0
}

func (x *TransactionRaw) GetSignature() string {
	if x != nil {
		return x.Signature
	}
	return ""
}

func (x *TransactionRaw) GetDataType() string {
	if x != nil {
		return x.DataType
	}
	return ""
}

func (x *TransactionRaw) GetData() string {
	if x != nil {
		return x.Data
	}
	return ""
}

func (x *TransactionRaw) GetReceiptCumulativeStepUsed() uint64 {
	if x != nil {
		return x.ReceiptCumulativeStepUsed
	}
	return 0
}

func (x *TransactionRaw) GetReceiptStepUsed() uint64 {
	if x != nil {
		return x.ReceiptStepUsed
	}
	return 0
}

func (x *TransactionRaw) GetReceiptStepPrice() uint64 {
	if x != nil {
		return x.ReceiptStepPrice
	}
	return 0
}

func (x *TransactionRaw) GetReceiptScoreAddress() string {
	if x != nil {
		return x.ReceiptScoreAddress
	}
	return ""
}

func (x *TransactionRaw) GetReceiptLogs() string {
	if x != nil {
		return x.ReceiptLogs
	}
	return ""
}

func (x *TransactionRaw) GetReceiptStatus() uint32 {
	if x != nil {
		return x.ReceiptStatus
	}
	return 0
}

func (x *TransactionRaw) GetItemId() string {
	if x != nil {
		return x.ItemId
	}
	return ""
}

func (x *TransactionRaw) GetItemTimestamp() string {
	if x != nil {
		return x.ItemTimestamp
	}
	return ""
}

var File_transaction_raw_proto protoreflect.FileDescriptor

var file_transaction_raw_proto_rawDesc = []byte{
	0x0a, 0x15, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x72, 0x61,
	0x77, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x73, 0x22,
	0xe1, 0x06, 0x0a, 0x0e, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x61, 0x77, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f,
	0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x12, 0x21, 0x0a, 0x0c, 0x66, 0x72, 0x6f, 0x6d, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x66, 0x72, 0x6f, 0x6d, 0x41, 0x64, 0x64, 0x72,
	0x65, 0x73, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73,
	0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x41, 0x64, 0x64, 0x72, 0x65,
	0x73, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x73, 0x74, 0x65, 0x70,
	0x5f, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x73, 0x74,
	0x65, 0x70, 0x4c, 0x69, 0x6d, 0x69, 0x74, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73,
	0x74, 0x61, 0x6d, 0x70, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x27, 0x0a, 0x0f, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x5f, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x08, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0e,
	0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x10,
	0x0a, 0x03, 0x6e, 0x69, 0x64, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x03, 0x6e, 0x69, 0x64,
	0x12, 0x14, 0x0a, 0x05, 0x6e, 0x6f, 0x6e, 0x63, 0x65, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x05, 0x6e, 0x6f, 0x6e, 0x63, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x0b,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x2b, 0x0a, 0x11, 0x74, 0x72,
	0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18,
	0x0c, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x10, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x61, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x1d, 0x0a, 0x0a, 0x62, 0x6c, 0x6f, 0x63, 0x6b,
	0x5f, 0x68, 0x61, 0x73, 0x68, 0x18, 0x0d, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x62, 0x6c, 0x6f,
	0x63, 0x6b, 0x48, 0x61, 0x73, 0x68, 0x12, 0x21, 0x0a, 0x0c, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x5f,
	0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x0e, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0b, 0x62, 0x6c,
	0x6f, 0x63, 0x6b, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x12, 0x10, 0x0a, 0x03, 0x66, 0x65, 0x65,
	0x18, 0x0f, 0x20, 0x01, 0x28, 0x04, 0x52, 0x03, 0x66, 0x65, 0x65, 0x12, 0x1c, 0x0a, 0x09, 0x73,
	0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x18, 0x10, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09,
	0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x12, 0x1b, 0x0a, 0x09, 0x64, 0x61, 0x74,
	0x61, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x11, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61,
	0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x12,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x12, 0x3f, 0x0a, 0x1c, 0x72, 0x65,
	0x63, 0x65, 0x69, 0x70, 0x74, 0x5f, 0x63, 0x75, 0x6d, 0x75, 0x6c, 0x61, 0x74, 0x69, 0x76, 0x65,
	0x5f, 0x73, 0x74, 0x65, 0x70, 0x5f, 0x75, 0x73, 0x65, 0x64, 0x18, 0x13, 0x20, 0x01, 0x28, 0x04,
	0x52, 0x19, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x43, 0x75, 0x6d, 0x75, 0x6c, 0x61, 0x74,
	0x69, 0x76, 0x65, 0x53, 0x74, 0x65, 0x70, 0x55, 0x73, 0x65, 0x64, 0x12, 0x2a, 0x0a, 0x11, 0x72,
	0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x5f, 0x73, 0x74, 0x65, 0x70, 0x5f, 0x75, 0x73, 0x65, 0x64,
	0x18, 0x14, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0f, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x53,
	0x74, 0x65, 0x70, 0x55, 0x73, 0x65, 0x64, 0x12, 0x2c, 0x0a, 0x12, 0x72, 0x65, 0x63, 0x65, 0x69,
	0x70, 0x74, 0x5f, 0x73, 0x74, 0x65, 0x70, 0x5f, 0x70, 0x72, 0x69, 0x63, 0x65, 0x18, 0x15, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x10, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x53, 0x74, 0x65, 0x70,
	0x50, 0x72, 0x69, 0x63, 0x65, 0x12, 0x32, 0x0a, 0x15, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74,
	0x5f, 0x73, 0x63, 0x6f, 0x72, 0x65, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x16,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x13, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x53, 0x63, 0x6f,
	0x72, 0x65, 0x41, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x12, 0x21, 0x0a, 0x0c, 0x72, 0x65, 0x63,
	0x65, 0x69, 0x70, 0x74, 0x5f, 0x6c, 0x6f, 0x67, 0x73, 0x18, 0x17, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0b, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x4c, 0x6f, 0x67, 0x73, 0x12, 0x25, 0x0a, 0x0e,
	0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x18,
	0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x72, 0x65, 0x63, 0x65, 0x69, 0x70, 0x74, 0x53, 0x74, 0x61,
	0x74, 0x75, 0x73, 0x12, 0x17, 0x0a, 0x07, 0x69, 0x74, 0x65, 0x6d, 0x5f, 0x69, 0x64, 0x18, 0x19,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x69, 0x74, 0x65, 0x6d, 0x49, 0x64, 0x12, 0x25, 0x0a, 0x0e,
	0x69, 0x74, 0x65, 0x6d, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x1a,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x69, 0x74, 0x65, 0x6d, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x42, 0x0a, 0x5a, 0x08, 0x2e, 0x2f, 0x6d, 0x6f, 0x64, 0x65, 0x6c, 0x73, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_transaction_raw_proto_rawDescOnce sync.Once
	file_transaction_raw_proto_rawDescData = file_transaction_raw_proto_rawDesc
)

func file_transaction_raw_proto_rawDescGZIP() []byte {
	file_transaction_raw_proto_rawDescOnce.Do(func() {
		file_transaction_raw_proto_rawDescData = protoimpl.X.CompressGZIP(file_transaction_raw_proto_rawDescData)
	})
	return file_transaction_raw_proto_rawDescData
}

var file_transaction_raw_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_transaction_raw_proto_goTypes = []interface{}{
	(*TransactionRaw)(nil), // 0: models.TransactionRaw
}
var file_transaction_raw_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_transaction_raw_proto_init() }
func file_transaction_raw_proto_init() {
	if File_transaction_raw_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_transaction_raw_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TransactionRaw); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_transaction_raw_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_transaction_raw_proto_goTypes,
		DependencyIndexes: file_transaction_raw_proto_depIdxs,
		MessageInfos:      file_transaction_raw_proto_msgTypes,
	}.Build()
	File_transaction_raw_proto = out.File
	file_transaction_raw_proto_rawDesc = nil
	file_transaction_raw_proto_goTypes = nil
	file_transaction_raw_proto_depIdxs = nil
}
