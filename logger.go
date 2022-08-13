package domain

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/hnhuaxi/platform/logger"
	"github.com/imdario/mergo"
	"github.com/jinzhu/copier"
	"go.uber.org/zap"
)

var Logger = watermill.NewStdLogger(false, false)

type llogger struct {
	log    *zap.SugaredLogger
	fields watermill.LogFields
}

func StdLogger(logger *logger.Logger) *llogger {
	return &llogger{
		log: logger.Sugar(),
	}
}

func (log *llogger) fieldsArgs(fields watermill.LogFields) []interface{} {
	var (
		args []interface{}
		m    = make(map[string]interface{})
	)

	if len(log.fields) > 0 {
		copier.Copy(&m, log.fields)
	}

	mergo.Map(&m, fields, mergo.WithOverride)

	for key, field := range m {
		args = append(args, key, field)
	}

	return args
}

func (log *llogger) Error(msg string, err error, fields watermill.LogFields) {
	log.log.Errorw(fmt.Sprintf(msg, err), log.fieldsArgs(fields)...)
}

func (log *llogger) Info(msg string, fields watermill.LogFields) {
	log.log.Infow(msg, log.fieldsArgs(fields)...)
}

func (log *llogger) Debug(msg string, fields watermill.LogFields) {
	log.log.Debugw(msg, log.fieldsArgs(fields)...)
}

func (log *llogger) Trace(msg string, fields watermill.LogFields) {
	log.log.Debugw(msg, log.fieldsArgs(fields)...)
}

func (log *llogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &llogger{
		log:    log.log,
		fields: fields,
	}
}
