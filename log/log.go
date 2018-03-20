package log

import "errors"

var factory Factory

// Setup set's up a new factory to the global state
func Setup(f Factory) error {
	if f == nil {
		return errors.New("factory is nil")
	}

	factory = f

	return nil
}

// Create returns a new logger
func Create() Logger {
	return factory.Create()
}

// CreateWithFields returns a new logger with fields
func CreateWithFields(fields map[string]interface{}) Logger {
	return factory.CreateWithFields(fields)
}

// CreateSub returns a new sub logger
func CreateSub(l Logger, fields map[string]interface{}) Logger {
	return factory.CreateSub(l, fields)
}
