package raft

import "encoding/json"

type CommandType string

const (
	CmdRegisterBroker CommandType = "RegisterBroker"
	CmdCreateTopic    CommandType = "CreateTopic"
	CmdDeleteTopic    CommandType = "DeleteTopic"
	CmdUpdateISR      CommandType = "UpdateISR"
)

type Command struct {
	Type    CommandType
	Payload json.RawMessage
}

type RegisterBrokerCommand struct {
	BrokerID int32
	Host     string
	Port     int32
	RaftAddr string
	RawAddr  string
}

type CreateTopicCommand struct {
	Topic             string
	NumPartitions     int32
	ReplicationFactor int32
}

type DeleteTopicCommand struct {
	Topic string
}

type UpdateISRCommand struct {
	Topic     string
	Partition int32
	ISR       []int32
}

func NewRegisterBrokerCommand(payload RegisterBrokerCommand) (Command, error) {
	return newCommand(CmdRegisterBroker, payload)
}

func NewCreateTopicCommand(payload CreateTopicCommand) (Command, error) {
	return newCommand(CmdCreateTopic, payload)
}

func NewDeleteTopicCommand(payload DeleteTopicCommand) (Command, error) {
	return newCommand(CmdDeleteTopic, payload)
}

func NewUpdateISRCommand(payload UpdateISRCommand) (Command, error) {
	return newCommand(CmdUpdateISR, payload)
}

func newCommand(t CommandType, payload interface{}) (Command, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return Command{}, err
	}
	return Command{Type: t, Payload: data}, nil
}

func (c Command) DecodeRegisterBroker() (RegisterBrokerCommand, error) {
	var payload RegisterBrokerCommand
	err := json.Unmarshal(c.Payload, &payload)
	return payload, err
}

func (c Command) DecodeCreateTopic() (CreateTopicCommand, error) {
	var payload CreateTopicCommand
	err := json.Unmarshal(c.Payload, &payload)
	return payload, err
}

func (c Command) DecodeDeleteTopic() (DeleteTopicCommand, error) {
	var payload DeleteTopicCommand
	err := json.Unmarshal(c.Payload, &payload)
	return payload, err
}

func (c Command) DecodeUpdateISR() (UpdateISRCommand, error) {
	var payload UpdateISRCommand
	err := json.Unmarshal(c.Payload, &payload)
	return payload, err
}

func Encode(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func Decode(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
