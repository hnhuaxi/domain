package domain

import "github.com/ThreeDotsLabs/watermill/components/cqrs"

var (
	JSONMarshaler     = cqrs.JSONMarshaler{}
	ProtobufMarshaler = cqrs.ProtobufMarshaler{}
)
