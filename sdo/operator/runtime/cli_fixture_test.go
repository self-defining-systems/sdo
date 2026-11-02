package runtime

import (
	"sync"
)

var cliFixture struct {
	once              sync.Once
	binary, directory string
	err               error
}

