// servicefixture holds a native executable open without starting CXP resources.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() { fmt.Println("ready"); io.Copy(io.Discard, os.Stdin) }
