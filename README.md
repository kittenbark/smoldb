# smoldb — human-readable and updatable with vi-editable db

```go
package main

import (
	"github.com/kittenbark/smoldb"
)

func main() {
	db, _ := smoldb.New[int, string]("hello.yaml")
	
	_ = db.Set(1, "hello world")
	
	hello, _ := db.Get(1)
	println(hello)
	
	_ = db.Del(1)
}
```