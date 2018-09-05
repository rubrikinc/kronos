/*
Package checksumfile can be used to persist data to a file so that it is never
corrupted. It only works on unix* systems as it writes data to a temp file and
uses rename syscall to move the file atomically.
An example is:
    func main() {
    	data := []byte{"Hello World!"}
    	filename := "var/lib/kronos/nodeInfo" // directory in which file is
    	// stored should exist beforehand and have w and x permissions for user,
     	// otherwise write will fail.
    	if err :=  checksumfile.Write(filename, data) {
    		panic(err)
    	}
    	read, err := checksumfile.Read(filename)
    	if err != nil {
    		panic(err)
    	}
      fmt.Println(read)
    }
*/
package checksumfile
